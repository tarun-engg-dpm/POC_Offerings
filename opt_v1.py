import os
import json
import random
import time
import datetime
from bson import ObjectId
import redis
# We still need the RedisCluster client for a sharded environment
from redis.cluster import RedisCluster, ClusterNode
from locust import User, task, between, events

# --- Configuration ---
REDIS_HOST = os.environ.get("REDIS_HOST", "dpm-offerings-clustered.e6nc9i.clustercfg.aps1.cache.amazonaws.com")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
OFFERS_JSON_FILE = os.environ.get("OFFERS_JSON_FILE", "resources/both_only.json")


# --- Global Data ---
OFFER_CONFIGS = {}
ALL_OFFER_IDS = []

# --- LUA SCRIPT: We only need the single-key script now ---
ATOMIC_INCR_THEN_CHECK_SINGLE_LUA = """
local k,c,ex=KEYS[1],tonumber(ARGV[1]),tonumber(ARGV[2]);local n=redis.call('INCR',k);if n<=c then if n==1 then redis.call('EXPIREAT',k,ex)end;return"SUCCESS"else redis.call('DECR',k);return"FAIL_CAP_MET"end
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

        self.atomic_script = self.client.register_script(ATOMIC_INCR_THEN_CHECK_SINGLE_LUA)
        self.expire_at_timestamp = int(
            (datetime.datetime.now() + datetime.timedelta(days=1)).replace(hour=1).timestamp())

    def _claim_offers_in_batch_iterative(self, user_id, batch_to_try):
        """The low-level claim function. Makes one or two atomic calls per offer."""
        granted = []
        date_str = datetime.datetime.now().strftime("%Y%m%d")
        for offer_id in batch_to_try:
            config = OFFER_CONFIGS.get(offer_id)
            if not config: continue

            offer_type = config.get("type")
            was_granted = False

            # --- THIS BLOCK USES THE CORRECTED KEY FORMATS ---
            if offer_type == "GLOBAL_ONLY":
                # The f-string '{{offer}}' correctly creates a literal '{offer}'
                key = f"{{offer}}:{offer_id}:{date_str}"
                if self.atomic_script(keys=[key],
                                      args=[config.get("global_cap"), self.expire_at_timestamp]) == b"SUCCESS":
                    was_granted = True

            elif offer_type == "USER_ONLY":
                key = f"user_offer:{offer_id}:{{{user_id}}}:{date_str}"
                if self.atomic_script(keys=[key],
                                      args=[config.get("user_cap", 1), self.expire_at_timestamp]) == b"SUCCESS":
                    was_granted = True

            elif offer_type == "BOTH":
                user_key = f"user_offer:{offer_id}:{{{user_id}}}:{date_str}"
                user_cap = config.get("user_cap", 1)
                user_result = self.atomic_script(keys=[user_key], args=[user_cap, self.expire_at_timestamp])

                if user_result == b"SUCCESS":
                    global_key = f"{{offer}}:{offer_id}:{date_str}"
                    global_cap = config.get("global_cap")
                    global_result = self.atomic_script(keys=[global_key], args=[global_cap, self.expire_at_timestamp])

                    if global_result == b"SUCCESS":
                        was_granted = True
                    else:
                        self.client.decr(user_key)

            if was_granted:
                granted.append(offer_id)

        return granted

    def _secure_n_offers_centralized_hotspot(self, user_id, applicable_offers_ids, n_required):
        """The "Filter-then-Claim" orchestration with the corrected key format."""
        current_date_str = datetime.datetime.now().strftime("%Y%m%d")

        keys_to_mget, key_to_offer_map = [], {}
        for offer_id in applicable_offers_ids:
            config = OFFER_CONFIGS.get(offer_id)
            if config and config.get("type") in ["GLOBAL_ONLY", "BOTH"]:
                # Corrected key format for MGET
                key = f"{{offer}}:{offer_id}:{current_date_str}"
                keys_to_mget.append(key)
                key_to_offer_map[key] = {"id": offer_id, "cap": config.get("global_cap")}

        capped_offer_ids = set()
        if keys_to_mget:
            try:
                # This MGET will now be a single command sent to the one hot shard
                counts = self.client.mget(keys_to_mget)
                for key, count in zip(keys_to_mget, counts):
                    if count and int(count) >= key_to_offer_map[key]["cap"]:
                        capped_offer_ids.add(key_to_offer_map[key]["id"])
            except redis.exceptions.RedisError:
                pass

        filtered_offers = [oid for oid in applicable_offers_ids if oid not in capped_offer_ids]

        claimed_set, attempted_set = set(), set()
        max_attempts = n_required + 5
        for _ in range(max_attempts):
            if len(claimed_set) >= n_required: break
            needed_count = n_required - len(claimed_set)
            available_to_try = list(set(filtered_offers) - attempted_set)
            if not available_to_try: break
            batch_to_try = available_to_try[:needed_count]
            attempted_set.update(batch_to_try)
            newly_claimed = self._claim_offers_in_batch_iterative(user_id, batch_to_try)
            if newly_claimed:
                claimed_set.update(newly_claimed)

        return list(claimed_set)

    @task
    def secure_n_offers_task(self):
        opts = self.environment.parsed_options
        k = min(opts.offers_to_evaluate, len(ALL_OFFER_IDS))
        user_id = str(ObjectId())
        offers_to_evaluate = random.sample(ALL_OFFER_IDS, k=k)

        name = "orchestrator:sharded_centralized_hotspot"
        start_time = time.time()
        try:
            granted = self._secure_n_offers_centralized_hotspot(user_id, offers_to_evaluate, opts.n_to_secure)
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