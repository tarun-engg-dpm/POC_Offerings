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

# --- Global Data (Loaded once at test start) ---
OFFER_CONFIGS = {}
ALL_OFFER_IDS = []

# --- LUA SCRIPTS: The Atomic Building Blocks ---
# For "BOTH" type offers, enabled by co-locating keys with hash tags.
ATOMIC_CLAIM_BOTH_LUA = """
local global_key=KEYS[1];local user_key=KEYS[2];local global_cap=tonumber(ARGV[1]);local user_cap=tonumber(ARGV[2]);local expire_at=tonumber(ARGV[3]);local global_count=tonumber(redis.call('GET',global_key)or 0);if global_count<global_cap then local user_count=tonumber(redis.call('GET',user_key)or 0);if user_count<user_cap then local new_global_val=redis.call('INCR',global_key);local new_user_val=redis.call('INCR',user_key);if new_global_val==1 then redis.call('EXPIREAT',global_key,expire_at)end;if new_user_val==1 then redis.call('EXPIREAT',user_key,expire_at)end;return"SUCCESS"else return"FAIL_USER_CAP_MET"end else return"FAIL_GLOBAL_CAP_MET"end
"""
# For "GLOBAL_ONLY" or "USER_ONLY" offers.
ATOMIC_CLAIM_SINGLE_KEY_LUA = """
local key=KEYS[1];local cap=tonumber(ARGV[1]);local expire_at=tonumber(ARGV[2]);if(tonumber(redis.call('GET',key)or 0))<cap then if redis.call('INCR',key)==1 then redis.call('EXPIREAT',key,expire_at)end;return"SUCCESS"else return"FAIL_CAP_MET"end
"""


# Note: Lua scripts are minified to reduce network payload size in the request.

@events.init_command_line_parser.add_listener
def add_custom_arguments(parser):
    """Adds command-line arguments to control the test run."""
    parser.add_argument(
        "--offers-to-evaluate", type=int, env_var="LOCUST_OFFERS_TO_EVALUATE", default=20,
        help="Number of offers to randomly select as the user's eligible pool."
    )
    parser.add_argument(
        "--n-to-secure", type=int, env_var="LOCUST_N_TO_SECURE", default=10,
        help="The number of offers each user task must successfully secure."
    )


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Loads offer configurations from the JSON file once."""
    global OFFER_CONFIGS, ALL_OFFER_IDS
    print(f"Loading offer configurations from '{OFFERS_JSON_FILE}'...")
    try:
        with open(OFFERS_JSON_FILE, 'r') as f:
            OFFER_CONFIGS = json.load(f)
        ALL_OFFER_IDS = list(OFFER_CONFIGS.keys())
        if len(ALL_OFFER_IDS) < environment.parsed_options.offers_to_evaluate:
            print("Error: Not enough offers in JSON file for the offers-to-evaluate count.")
            environment.runner.quit()
        else:
            print(f"Successfully loaded {len(ALL_OFFER_IDS)} offer configurations.")
    except Exception as e:
        print(f"Fatal error loading configuration file: {e}")
        environment.runner.quit()


class RedisUser(User):
    """
    Simulates a user securing 'N' offers using the final, optimal, cluster-safe architecture.
    """
    wait_time = between(0.05, 0.2)  # Wait 50-200ms between tasks

    def on_start(self):
        """Called once per user. Connects to the Redis Cluster and registers scripts."""
        if not ALL_OFFER_IDS:
            self.environment.runner.quit()
            return

        try:
            print(f"Connecting to Redis Cluster with startup node {REDIS_HOST}:{REDIS_PORT}")
            startup_nodes = [ClusterNode(REDIS_HOST, REDIS_PORT)]
            self.client = RedisCluster(
                startup_nodes=startup_nodes,
                decode_responses=False,  # Lua scripts return bytes, which is faster
                skip_full_coverage_check=True
            )
            self.client.ping()
        except Exception as e:
            print(f"Fatal: Could not connect to Redis Cluster. Error: {e}")
            self.environment.runner.quit()

        # Register Lua scripts once per user for efficiency
        self.scripts = {
            'both': self.client.register_script(ATOMIC_CLAIM_BOTH_LUA),
            'single': self.client.register_script(ATOMIC_CLAIM_SINGLE_KEY_LUA)
        }
        # Pre-calculate the expiration timestamp once per user start
        self.expire_at_timestamp = int(
            (datetime.datetime.now() + datetime.timedelta(days=1)).replace(hour=1, minute=0, second=0,
                                                                           microsecond=0).timestamp())

    def _claim_offers_in_batch(self, user_id, batch_to_try):
        """The low-level, cluster-safe claim function using atomic Lua calls."""
        granted_in_this_batch = []
        current_date_str = datetime.datetime.now().strftime("%Y%m%d")

        for offer_id in batch_to_try:
            config = OFFER_CONFIGS.get(offer_id)
            if not config: continue

            offer_type = config.get("type")
            was_granted = False

            # if offer_type == "GLOBAL_ONLY":
            #     key = f"offer:{{{offer_id}}}:count:{current_date_str}"
            #     cap = config.get("global_cap")
            #     if self.scripts['single'](keys=[key], args=[cap, self.expire_at_timestamp]) == b"SUCCESS":
            #         was_granted = True
            #
            # elif offer_type == "USER_ONLY":
            #     key = f"user_offer:{{{offer_id}}}:count:{user_id}:{current_date_str}"
            #     cap = config.get("user_cap", 1)
            #     if self.scripts['single'](keys=[key], args=[cap, self.expire_at_timestamp]) == b"SUCCESS":
            #         was_granted = True

            if offer_type == "BOTH":
                global_key = f"offer:{{{offer_id}}}:count:{current_date_str}"
                user_key = f"user_offer:{{{offer_id}}}:count:{user_id}:{current_date_str}"
                global_cap = config.get("global_cap")
                user_cap = config.get("user_cap", 1)
                if self.scripts['both'](keys=[global_key, user_key],
                                        args=[global_cap, user_cap, self.expire_at_timestamp]) == b"SUCCESS":
                    was_granted = True

            if was_granted:
                granted_in_this_batch.append(offer_id)

        return granted_in_this_batch

    def _secure_n_offers(self, user_id, offers_to_evaluate, n_required):
        """The high-level orchestration logic being tested."""
        eligible_pool = set(offers_to_evaluate)
        claimed_set = set()
        attempted_set = set()

        # A dynamic and resilient circuit breaker
        max_attempts = n_required + 5

        for _ in range(max_attempts):
            if len(claimed_set) >= n_required: break

            needed_count = n_required - len(claimed_set)
            available_to_try = list(eligible_pool - attempted_set)
            if not available_to_try: break

            batch_to_try = available_to_try[:needed_count]
            attempted_set.update(batch_to_try)

            newly_claimed = self._claim_offers_in_batch(user_id, batch_to_try)
            if newly_claimed:
                claimed_set.update(newly_claimed)

        return list(claimed_set)

    @task
    def secure_n_offers_task(self):
        """The main Locust task that simulates the entire user journey."""
        offers_to_evaluate_count = self.environment.parsed_options.offers_to_evaluate
        n_to_secure = self.environment.parsed_options.n_to_secure

        k = min(offers_to_evaluate_count, len(ALL_OFFER_IDS))
        user_id = str(ObjectId())
        offers_to_evaluate = random.sample(ALL_OFFER_IDS, k=k)

        request_name = "orchestrator:secure_n_offers_optimal"
        start_time = time.time()
        try:
            final_granted_offers = self._secure_n_offers(user_id, offers_to_evaluate, n_to_secure)
            total_time = int((time.time() - start_time) * 1000)

            if len(final_granted_offers) < n_to_secure:
                events.request.fire(
                    request_type="redis", name=request_name, response_time=total_time,
                    response_length=len(str(final_granted_offers)),
                    exception=f"Could not secure all {n_to_secure} offers",
                )
            else:
                events.request.fire(
                    request_type="redis", name=request_name, response_time=total_time,
                    response_length=len(str(final_granted_offers)), exception=None, context={"user_id": user_id}
                )
        except redis.exceptions.RedisError as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="redis", name=request_name, response_time=total_time,
                response_length=0, exception=e
            )
            # locust -f batch_locust.py --offers-to-evaluate 20 --n-to-secure 10