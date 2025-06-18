import os
import json
import random
import time
import datetime
from bson import ObjectId
import redis
# Import the necessary classes for a sharded Redis Cluster
from redis.cluster import RedisCluster, ClusterNode
from locust import User, task, between, events

# --- Configuration ---
REDIS_HOST = os.environ.get("REDIS_HOST", "dpm-offerings-clustered.e6nc9i.clustercfg.aps1.cache.amazonaws.com")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
OFFERS_JSON_FILE = os.environ.get("OFFERS_JSON_FILE", "resources/both_only.json")


# --- Global Data ---
OFFER_CONFIGS = {}
ALL_OFFER_IDS = []

# --- LUA SCRIPTS (minified for performance) ---
# "No-DECR" logic for single-key offers
ATOMIC_CLAIM_SINGLE_NO_DECR_LUA = """
local k,c,ex=KEYS[1],tonumber(ARGV[1]),tonumber(ARGV[2]);local n=redis.call('INCR',k);if n<=c then if n==1 then redis.call('EXPIREAT',k,ex)end;return"SUCCESS"else return"FAIL_CAP_MET"end
"""
# Conditional DECR logic for BOTH offers
ATOMIC_CLAIM_BOTH_REDUCED_DECR_LUA = """
local g,u,gc,uc,ex=KEYS[1],KEYS[2],tonumber(ARGV[1]),tonumber(ARGV[2]),tonumber(ARGV[3]);local n_g=redis.call('INCR',g);if n_g<=gc then local n_u=redis.call('INCR',u);if n_u<=uc then if n_g==1 then redis.call('EXPIREAT',g,ex)end;if n_u==1 then redis.call('EXPIREAT',u,ex)end;return"SUCCESS"else redis.call('DECR',g);return"FAIL_USER_CAP_MET"end else return"FAIL_GLOBAL_CAP_MET"end
"""


@events.init_command_line_parser.add_listener
def add_custom_arguments(parser):
    parser.add_argument("--offers-to-evaluate", type=int, env_var="LOCUST_OFFERS_TO_EVALUATE", default=20)
    parser.add_argument("--n-to-secure", type=int, env_var="LOCUST_N_TO_SECURE", default=10)


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    global OFFER_CONFIGS, ALL_OFFER_IDS
    print(f"Loading offers from '{OFFERS_JSON_FILE}'...")
    try:
        with open(OFFERS_JSON_FILE, 'r') as f:
            OFFER_CONFIGS = json.load(f)
        ALL_OFFER_IDS = list(OFFER_CONFIGS.keys())
        if len(ALL_OFFER_IDS) < environment.parsed_options.offers_to_evaluate:
            environment.runner.quit()
    except Exception as e:
        print(f"Fatal error loading config: {e}");
        environment.runner.quit()


class RedisUser(User):
    wait_time = between(0.01, 0.05)

    def on_start(self):
        if not ALL_OFFER_IDS: self.environment.runner.quit(); return
        try:
            print(f"Connecting to Redis Cluster: {REDIS_HOST}:{REDIS_PORT}")
            nodes = [ClusterNode(REDIS_HOST, REDIS_PORT)]
            self.client = RedisCluster(startup_nodes=nodes, decode_responses=False, skip_full_coverage_check=True)
            self.client.ping()
        except Exception as e:
            print(f"Fatal connection error: {e}");
            self.environment.runner.quit()

        self.scripts = {
            'both': self.client.register_script(ATOMIC_CLAIM_BOTH_REDUCED_DECR_LUA),
            'single': self.client.register_script(ATOMIC_CLAIM_SINGLE_NO_DECR_LUA)
        }
        self.expire_at_timestamp = int(
            (datetime.datetime.now() + datetime.timedelta(days=1)).replace(hour=1).timestamp())

    def _claim_offers_optimized(self, user_id, applicable_offers_ids, grant_limit):
        """
        (OPTIMIZED UNIFIED LOOP)
        Processes offers iteratively until the grant limit is reached or the list is exhausted.
        This is the most direct and efficient implementation of the required logic.
        """
        granted_offers = []
        # --- Optimization: Pre-compute date and timestamp once per task run ---
        current_date_str = datetime.datetime.now().strftime("%Y%m%d")
        expire_timestamp = self.expire_at_timestamp  # Use the pre-calculated one

        # --- Optimization: A single, clean loop ---
        for offer_id in applicable_offers_ids:
            # --- Optimization: Primary termination condition is checked first ---
            if len(granted_offers) >= grant_limit:
                break

            config = OFFER_CONFIGS.get(offer_id)
            if not config: continue

            offer_type = config.get("type")
            was_granted = False

            # This architecture assumes co-located keys using {offer_id} hash tags
            if offer_type == "GLOBAL_ONLY":
                key = f"offer:{{{offer_id}}}:count:{current_date_str}"
                if self.scripts['single'](keys=[key], args=[config.get("global_cap"), expire_timestamp]) == b"SUCCESS":
                    was_granted = True
            elif offer_type == "USER_ONLY":
                key = f"user_offer:{{{offer_id}}}:count:{user_id}:{current_date_str}"
                if self.scripts['single'](keys=[key], args=[config.get("user_cap", 1), expire_timestamp]) == b"SUCCESS":
                    was_granted = True
            elif offer_type == "BOTH":
                g_key = f"offer:{{{offer_id}}}:count:{current_date_str}"
                u_key = f"user_offer:{{{offer_id}}}:count:{user_id}:{current_date_str}"
                if self.scripts['both'](keys=[g_key, u_key], args=[config.get("global_cap"), config.get("user_cap", 1),
                                                                   expire_timestamp]) == b"SUCCESS":
                    was_granted = True

            if was_granted:
                granted_offers.append(offer_id)

        return granted_offers

    @task
    def secure_n_offers_task(self):
        opts = self.environment.parsed_options
        k = min(opts.offers_to_evaluate, len(ALL_OFFER_IDS))
        user_id = str(ObjectId())
        offers_to_evaluate = random.sample(ALL_OFFER_IDS, k=k)

        name = "orchestrator:sharded_optimized_loop"
        start_time = time.time()
        try:
            # The main task now calls the single, optimized function
            granted = self._claim_offers_optimized(user_id, offers_to_evaluate, opts.n_to_secure)
            total_time = int((time.time() - start_time) * 1000)

            if len(granted) < opts.n_to_secure:
                events.request.fire(request_type="redis", name=name, response_time=total_time,
                                    exception=f"Failed to secure {opts.n_to_secure} offers")
            else:
                events.request.fire(request_type="redis", name=name, response_time=total_time,
                                    response_length=len(str(granted)), context={"user_id": user_id})
        except redis.exceptions.RedisError as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(request_type="redis", name=name, response_time=total_time, response_length=0,
                                exception=e)
            # locust -f opt_v2.py --offers-to-evaluate 20 --n-to-secure 10 --web-port 8090