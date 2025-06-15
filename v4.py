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

# --- LUA SCRIPTS: Our Atomic Building Blocks ---
# The most powerful script for the "BOTH" case, enabled by hash tags
ATOMIC_CLAIM_BOTH_LUA = """
local global_key = KEYS[1]
local user_key = KEYS[2]
local global_cap = tonumber(ARGV[1])
local user_cap = tonumber(ARGV[2])
local expire_at = tonumber(ARGV[3])
local global_count = tonumber(redis.call('GET', global_key) or 0)
if global_count < global_cap then
  local user_count = tonumber(redis.call('GET', user_key) or 0)
  if user_count < user_cap then
    local new_global_val = redis.call('INCR', global_key)
    local new_user_val = redis.call('INCR', user_key)
    if new_global_val == 1 then redis.call('EXPIREAT', global_key, expire_at) end
    if new_user_val == 1 then redis.call('EXPIREAT', user_key, expire_at) end
    return "SUCCESS"
  else return "FAIL_USER_CAP_MET" end
else return "FAIL_GLOBAL_CAP_MET" end
"""
# A simpler script for single-key checks
ATOMIC_CLAIM_SINGLE_KEY_LUA = """
local key = KEYS[1]
local cap = tonumber(ARGV[1])
local expire_at = tonumber(ARGV[2])
if (tonumber(redis.call('GET', key) or 0)) < cap then
  if redis.call('INCR', key) == 1 then redis.call('EXPIREAT', key, expire_at) end
  return "SUCCESS"
else return "FAIL_CAP_MET" end
"""


@events.init_command_line_parser.add_listener
def add_custom_arguments(parser):
    """Adds command-line arguments to control the test run."""
    parser.add_argument(
        "--offers-to-sample", type=int, env_var="LOCUST_OFFERS_TO_SAMPLE", default=20,
        help="Number of offers to randomly select as the user's applicable pool."
    )
    parser.add_argument(
        "--grant-limit", type=int, env_var="LOCUST_GRANT_LIMIT", default=10,
        help="The maximum number of offers to actually grant and return per task."
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
        if len(ALL_OFFER_IDS) < environment.parsed_options.offers_to_sample:
            print("Error: Not enough offers in JSON file for the offers-to-sample count.")
            environment.runner.quit()
        else:
            print(f"Successfully loaded {len(ALL_OFFER_IDS)} offer configurations.")
    except Exception as e:
        print(f"Fatal error loading configuration file: {e}")
        environment.runner.quit()


class RedisUser(User):
    """
    Simulates a user claiming offers using the final, optimal, cluster-safe method.
    """
    wait_time = between(0.01, 0.02)

    def on_start(self):
        """Called once per user. Connects to Redis Cluster and registers scripts."""
        if not ALL_OFFER_IDS: self.environment.runner.quit(); return

        try:
            print(f"Connecting to Redis Cluster with startup node {REDIS_HOST}:{REDIS_PORT}")
            startup_nodes = [ClusterNode(REDIS_HOST, REDIS_PORT)]
            self.client = RedisCluster(
                startup_nodes=startup_nodes,
                decode_responses=False,  # Lua scripts return bytes
                skip_full_coverage_check=True
            )
            self.client.ping()
            print("Successfully connected to Redis Cluster.")
        except Exception as e:
            print(f"Fatal: Could not connect to Redis Cluster. Error: {e}")
            self.environment.runner.quit()

        self.scripts = {
            'both': self.client.register_script(ATOMIC_CLAIM_BOTH_LUA),
            'single': self.client.register_script(ATOMIC_CLAIM_SINGLE_KEY_LUA)
        }
        self.expire_at_timestamp = int(
            (datetime.datetime.now() + datetime.timedelta(days=1)).replace(hour=1, minute=0, second=0,
                                                                           microsecond=0).timestamp())

    def _claim_offers_optimal_cluster(self, user_id, applicable_offers_ids, grant_limit):
        """The core logic being tested, using co-located keys and atomic scripts."""
        offers_to_return = []
        current_date_str = datetime.datetime.now().strftime("%Y%m%d")

        for offer_id in applicable_offers_ids:
            if len(offers_to_return) >= grant_limit: break

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
                offers_to_return.append(offer_id)

        return offers_to_return

    @task
    def claim_offers_task(self):
        """The main Locust task that simulates the user action."""
        offers_to_sample_count = self.environment.parsed_options.offers_to_sample
        grant_limit = self.environment.parsed_options.grant_limit

        k = min(offers_to_sample_count, len(ALL_OFFER_IDS))
        user_id = str(ObjectId())
        applicable_offers = random.sample(ALL_OFFER_IDS, k=k)

        request_name = "lua_optimal_cluster:claim_offers"
        start_time = time.time()
        try:
            granted_offers = self._claim_offers_optimal_cluster(user_id, applicable_offers, grant_limit)
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="redis", name=request_name, response_time=total_time,
                response_length=len(granted_offers), exception=None, context={"user_id": user_id}
            )
        except redis.exceptions.RedisError as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="redis", name=request_name, response_time=total_time,
                response_length=0, exception=e
            )
            # locust -f v4.py --offers-to-sample 20 --grant-limit 10