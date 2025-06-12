import os
import json
import random
import time
import datetime
import redis
from locust import User, task, between, events
from bson import ObjectId

# --- Configuration ---
REDIS_HOST = os.environ.get("REDIS_HOST", "dpm-offerings-clustered.e6nc9i.clustercfg.aps1.cache.amazonaws.com")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
# Ensure your JSON file is a dictionary like: {"offer_id_1": {"global_cap": X, "user_cap": Y}, ...}
OFFERS_JSON_FILE = os.environ.get("OFFERS_JSON_FILE", "resources/offers_db.json")

# --- Global variables, loaded once at the start of the test ---
OFFER_CONFIGS = {}
ALL_OFFER_IDS = []

# --- The Optimized and Robust Lua Script ---
# This version is superior as it uses a single MGET for all reads,
# significantly reducing overhead under high load.
BATCH_CLAIM_OPTIMIZED_LUA = """
--[[
  Atomically claims offers up to a limit, using a single MGET call for efficiency.
  This version avoids string manipulation by remembering the *indices* of valid offers.
--]]
local all_current_counts = redis.call('MGET', unpack(KEYS))
local valid_offer_indices = {}
local granted_count = 0
local grant_limit = tonumber(ARGV[#ARGV-1])
for i = 1, #KEYS / 2 do
  if granted_count >= grant_limit then break end
  local offer_key_index = (i * 2) - 1
  local user_key_index = i * 2
  local offer_cap = tonumber(ARGV[offer_key_index])
  local user_cap = tonumber(ARGV[user_key_index])
  local offer_count = tonumber(all_current_counts[offer_key_index] or 0)
  local user_count = tonumber(all_current_counts[user_key_index] or 0)
  if offer_count < offer_cap and user_count < user_cap then
    table.insert(valid_offer_indices, i)
    granted_count = granted_count + 1
  end
end
local final_granted_keys = {}
local expire_at_timestamp = tonumber(ARGV[#ARGV])
for _, i in ipairs(valid_offer_indices) do
  local offer_key_index = (i * 2) - 1
  local user_key_index = i * 2
  local offer_key = KEYS[offer_key_index]
  local user_key = KEYS[user_key_index]
  local new_offer_count = redis.call('INCR', offer_key)
  local new_user_count = redis.call('INCR', user_key)
  table.insert(final_granted_keys, offer_key)
  if new_offer_count == 1 then redis.call('EXPIREAT', offer_key, expire_at_timestamp) end
  if new_user_count == 1 then redis.call('EXPIREAT', user_key, expire_at_timestamp) end
end
return final_granted_keys
"""

@events.init_command_line_parser.add_listener
def add_custom_arguments(parser):
    """Adds custom command-line arguments to control the test run."""
    parser.add_argument(
        "--offers-to-sample", type=int, env_var="LOCUST_OFFERS_TO_SAMPLE", default=10,
        help="Number of random offers each user task should attempt to claim."
    )
    parser.add_argument(
        "--grant-limit", type=int, env_var="LOCUST_GRANT_LIMIT", default=3,
        help="The maximum number of offers to grant per user task."
    )

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Loads offer configurations from the JSON file once when the test begins."""
    global OFFER_CONFIGS, ALL_OFFER_IDS
    print(f"Loading offer configurations from '{OFFERS_JSON_FILE}'...")
    try:
        with open(OFFERS_JSON_FILE, 'r') as f:
            OFFER_CONFIGS = json.load(f)
        ALL_OFFER_IDS = list(OFFER_CONFIGS.keys())

        if not ALL_OFFER_IDS:
            print(f"Error: The offers file '{OFFERS_JSON_FILE}' is empty or invalid.")
            environment.runner.quit()
        else:
            print(f"Successfully loaded {len(ALL_OFFER_IDS)} offer configurations.")
    except Exception as e:
        print(f"Fatal error loading configuration file: {e}")
        environment.runner.quit()


class RedisUser(User):
    """Simulates a user claiming a limited bundle of offers using the optimized script."""
    wait_time = between(0.01, 0.1)

    def on_start(self):
        """Called once per user. Connects to Redis and registers the Lua script."""
        if not ALL_OFFER_IDS:
            self.environment.runner.quit()
            return

        print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
        try:
            # For ElastiCache Cluster, use redis.cluster.RedisCluster
            self.client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            self.client.ping()
        except redis.exceptions.ConnectionError as e:
            print(f"Fatal: Could not connect to Redis. Error: {e}")
            self.environment.runner.quit()

        # Register the single, optimized Lua script
        self.claim_script = self.client.register_script(BATCH_CLAIM_OPTIMIZED_LUA)

    @task
    def claim_offer_bundle_task(self):
        """The main Locust task that calls the Lua script and reports results."""
        user_id = str(ObjectId())
        offers_to_sample_count = self.environment.parsed_options.offers_to_sample
        grant_limit = self.environment.parsed_options.grant_limit

        # Sample a random batch of offers to try
        offers_to_try_ids = random.sample(ALL_OFFER_IDS, k=offers_to_sample_count)

        # --- Prepare Keys and Arguments for the Lua script ---
        keys_list = []
        args_list = []
        current_date_str = datetime.datetime.now().strftime("%Y%m%d")

        for offer_id in offers_to_try_ids:
            config = OFFER_CONFIGS.get(offer_id)
            if not config: continue

            # {tag} is a hint for Redis Cluster to place both keys on the same node
            offer_key = f"offer:{offer_id}:{current_date_str}"
            user_key = f"user_offer:{{{user_id}}}:{offer_id}:{current_date_str}"
            keys_list.extend([offer_key, user_key])
            args_list.extend([config.get('global_cap', 1), config.get('user_cap', 1)])

        # --- Calculate expiration and add final arguments in the correct order ---
        tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
        expire_datetime = tomorrow.replace(hour=1, minute=0, second=0, microsecond=0)
        expire_at_timestamp = int(expire_datetime.timestamp())

        # The order here is critical and must match the Lua script's expectations
        args_list.append(grant_limit)
        args_list.append(expire_at_timestamp)

        # --- Execute the script and measure performance ---
        request_name = "lua:batch_claim_optimized"
        start_time = time.time()
        try:
            granted_keys = self.claim_script(keys=keys_list, args=args_list)
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="redis", name=request_name, response_time=total_time,
                response_length=len(granted_keys), exception=None, context={"user_id": user_id}
            )
        except redis.exceptions.RedisError as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="redis", name=request_name, response_time=total_time,
                response_length=0, exception=e
            )