import os
import json
import random
import time
import datetime
import redis
from locust import User, task, between, events
from bson import ObjectId

# --- Configuration (from your script's comments) ---

REDIS_HOST = os.environ.get("REDIS_HOST", "dpm-offerings-cache.e6nc9i.clustercfg.aps1.cache.amazonaws.com")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
OFFERS_JSON_FILE = os.environ.get("OFFERS_JSON_FILE", "resources/both_only.json")
# export OFFERS_JSON_FILE="resources/user_only.json"
# export OFFERS_JSON_FILE="resources/global_only.json"
# export OFFERS_JSON_FILE="resources/both_only.json"
OFFERS_TO_SAMPLE_PER_TASK = 10  # How many offers each user will try to claim

# --- Global variables to hold offer data ---
OFFER_CONFIGS = {}
ALL_OFFER_IDS = []



@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """
    Called once when the Locust test starts. Loads offer configurations from the JSON file.
    """
    global OFFER_CONFIGS, ALL_OFFER_IDS

    print(f"Loading offer configurations from '{OFFERS_JSON_FILE}'...")
    try:
        with open(OFFERS_JSON_FILE, 'r') as f:
            OFFER_CONFIGS = json.load(f)

        # Create a list of all offer IDs for easy random sampling
        ALL_OFFER_IDS = list(OFFER_CONFIGS.keys())

        if not ALL_OFFER_IDS or len(ALL_OFFER_IDS) < OFFERS_TO_SAMPLE_PER_TASK:
            print(f"Error: The offers file must exist and contain at least {OFFERS_TO_SAMPLE_PER_TASK} offers.")
            environment.runner.quit()
        else:
            print(f"Successfully loaded {len(ALL_OFFER_IDS)} offer configurations.")
    except Exception as e:
        print(f"Fatal error loading configuration file: {e}")
        environment.runner.quit()


class RedisUser(User):
    """
    Simulates a user claiming a bundle of offers using the two-pipeline strategy.
    """
    wait_time = between(0.01, .02)

    # --- LUA SCRIPTS (as defined in your original file) ---
    GLOBAL_ONLY_LUA_SCRIPT = """
        -- KEYS[1]: offer_key, ARGV[1]: offer_cap, ARGV[2]: expire_at_timestamp
        if (tonumber(redis.call('GET', KEYS[1]) or 0)) < tonumber(ARGV[1]) then
          if redis.call('INCR', KEYS[1]) == 1 then redis.call('EXPIREAT', KEYS[1], tonumber(ARGV[2])) end
          return "SUCCESS"
        else return "FAIL_CAP_MET" end
    """
    USER_ONLY_LUA_SCRIPT = """
        -- KEYS[1]: user_key, ARGV[1]: user_cap, ARGV[2]: expire_at_timestamp
        if (tonumber(redis.call('GET', KEYS[1]) or 0)) < tonumber(ARGV[1]) then
          if redis.call('INCR', KEYS[1]) == 1 then redis.call('EXPIREAT', KEYS[1], tonumber(ARGV[2])) end
          return "SUCCESS"
        else return "FAIL_USER_CAP_MET" end
    """

    def log_redis_keys(self, client, pattern="*"):
        """
        Logs Redis keys matching the given pattern.
        :param client: Redis client instance
        :param pattern: Pattern to match keys (default is '*', which matches all keys)
        """
        try:
            keys = client.keys(pattern)
            print(f"Found {len(keys)} keys matching pattern '{pattern}':")
            for key in keys[:10]:  # Log only the first 10 keys
                print(key.decode('utf-8'))  # Decode bytes to string
        except redis.exceptions.RedisError as e:
            print(f"Error fetching keys: {e}")

    def on_start(self):
        """
        Called once for each simulated user when they start.
        """
        if not ALL_OFFER_IDS:
            self.environment.runner.quit()
            return

        print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
        try:
            # For ElastiCache Cluster, you would use redis.cluster.RedisCluster
            # For a standalone instance, redis.Redis is correct.
            self.client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
            self.client.ping()
            # self.log_redis_keys(self.client, pattern="offer:*")
        except redis.exceptions.ConnectionError as e:
            print(f"Fatal: Could not connect to Redis. Aborting test. Error: {e}")
            self.environment.runner.quit()

        # Register Lua scripts once per user for efficiency
        self.global_script = self.client.register_script(self.GLOBAL_ONLY_LUA_SCRIPT)
        self.user_script = self.client.register_script(self.USER_ONLY_LUA_SCRIPT)

    def claim_offers_flexible(self, user_id, offers_to_try):
        """
        This is the core logic from your script, adapted as a class method.
        All 'print' statements have been removed for performance.
        """
        current_date_str = datetime.datetime.now().strftime("%Y%m%d")
        random_seconds = random.randint(0, 1800)
        tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
        expire_datetime = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(
            seconds=random_seconds)
        expire_at_timestamp = int(expire_datetime.timestamp())
        offers_for_global_incr = []
        results_summary = {}

        # --- Pipeline 1: Handle all User-Level and Global-Only checks ---
        pipe = self.client.pipeline(transaction=False)
        valid_offers_in_pipeline1 = []
        for offer_id in offers_to_try:
            config = OFFER_CONFIGS.get(offer_id)
            if not config: continue

            # valid_offers_in_pipeline1.append(offer_id)
            cap_type = config['type']

            # if cap_type == 'GLOBAL_ONLY':
            #     key = f"offer:{offer_id}:{current_date_str}"
            #     args = [config['global_cap'], expire_at_timestamp]
            #     self.global_script(keys=[key], args=args, client=pipe)
            #     valid_offers_in_pipeline1.append(offer_id)
            #
            # if cap_type in ['USER_ONLY']:
            #     key = f"user_offer:{{{user_id}}}:{offer_id}:{current_date_str}"
            #     args = [config['user_cap'], expire_at_timestamp]
            #     self.user_script(keys=[key], args=args, client=pipe)
            #     valid_offers_in_pipeline1.append(offer_id)

            if cap_type in ['BOTH']:
                key = f"user_offer:{{{user_id}}}:{offer_id}:{current_date_str}"
                args = [config['user_cap'], expire_at_timestamp]
                self.user_script(keys=[key], args=args, client=pipe)
                valid_offers_in_pipeline1.append(offer_id)

        if not valid_offers_in_pipeline1:
            return []  # No valid offers to process

        pipeline1_results = pipe.execute()

        # --- Process results of Pipeline 1 and prepare Pipeline 2 ---
        for i, offer_id in enumerate(valid_offers_in_pipeline1):
            config = OFFER_CONFIGS[offer_id]
            result = pipeline1_results[i]

            if result == b'SUCCESS':
                if config['type'] == 'BOTH':
                    offers_for_global_incr.append(offer_id)
                results_summary[offer_id] = 'SUCCESS'
            else:
                results_summary[offer_id] = 'FAIL'

        # --- Pipeline 2: Handle Best-Effort Global Increments ---
        if offers_for_global_incr:
            pipe2 = self.client.pipeline(transaction=False)
            for offer_id in offers_for_global_incr:
                pipe2.incr(f"offer:{offer_id}:{current_date_str}")
            pipe2.execute()

        return [offer_id for offer_id, status in results_summary.items() if status == 'SUCCESS']

    @task
    def claim_offer_bundle_task(self):
        """
        The main Locust task. It wraps the call to the pipeline logic and measures performance.
        """
        user_id = str(ObjectId())
        offers_to_try = random.sample(ALL_OFFER_IDS, k=OFFERS_TO_SAMPLE_PER_TASK)

        request_name = "pipeline:claim_offers"
        start_time = time.time()
        try:
            granted_offers = self.claim_offers_flexible(user_id, offers_to_try)
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="redis",
                name=request_name,
                response_time=total_time,
                response_length=len(granted_offers),
                exception=None,
                context={"user_id": user_id}
            )
        except redis.exceptions.RedisError as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="redis",
                name=request_name,
                response_time=total_time,
                response_length=0,
                exception=e
            )
