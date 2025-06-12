import os
import json
import random
import time
import datetime
from bson import ObjectId
import redis
from locust import User, task, between, events

# --- Configuration ---
REDIS_HOST = os.environ.get("REDIS_HOST", "dpm-offerings-clustered.e6nc9i.clustercfg.aps1.cache.amazonaws.com")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
# Your JSON file with offer configs: {"offer_id": {"type": "...", "global_cap": ...}, ...}
OFFERS_JSON_FILE = os.environ.get("OFFERS_JSON_FILE", "resources/offers_db.json")

# --- Global variables, loaded once at the start of the test ---
OFFER_CONFIGS = {}
ALL_OFFER_IDS = []

# --- The "Optimizer" Lua Script that makes each step atomic ---
ATOMIC_CHECK_AND_INCR_LUA = """
local key = KEYS[1]
local cap = tonumber(ARGV[1])
local expire_at = tonumber(ARGV[2])
local current_count = tonumber(redis.call('GET', key) or 0)
if current_count < cap then
  local new_count = redis.call('INCR', key)
  if new_count == 1 then redis.call('EXPIREAT', key, expire_at) end
  return "SUCCESS"
else
  return "FAIL_CAP_MET"
end
"""


def get_next_day_midnight_timestamp():
    """Calculates the UNIX timestamp for 00:00:00 UTC of the next day."""
    tomorrow = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
    return int(tomorrow.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())


@events.init_command_line_parser.add_listener
def add_custom_arguments(parser):
    """Adds custom command-line arguments to control the test run."""
    parser.add_argument(
        "--offers-to-sample", type=int, env_var="LOCUST_OFFERS_TO_SAMPLE", default=10,
        help="Number of offers from the JSON file to randomly select for each task."
    )
    parser.add_argument(
        "--grant-limit", type=int, env_var="LOCUST_GRANT_LIMIT", default=3,
        help="The maximum number of offers to actually grant and return per task."
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
    """
    Simulates a user claiming offers using the iterative atomic method.
    """
    wait_time = between(0.05, 0.2)  # Wait 50-200ms between tasks

    def on_start(self):
        """Called once per user. Connects to Redis and registers the Lua script."""
        if not ALL_OFFER_IDS:
            self.environment.runner.quit()
            return

        print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
        try:
            self.client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            self.client.ping()
        except redis.exceptions.ConnectionError as e:
            print(f"Fatal: Could not connect to Redis. Error: {e}")
            self.environment.runner.quit()

        # Register the reusable Lua script once per user for efficiency
        self.atomic_check_script = self.client.register_script(ATOMIC_CHECK_AND_INCR_LUA)

    def _claim_offers_iterative_atomic(self, user_id, applicable_offers_ids, grant_limit):
        """
        The core logic being tested. This is a method of the User class now
        for easy access to self.client and self.atomic_check_script.
        """
        offers_to_return = []
        current_date_str = datetime.datetime.now().strftime("%Y%m%d")
        expire_timestamp = get_next_day_midnight_timestamp()

        for offer_id in applicable_offers_ids:
            if len(offers_to_return) >= grant_limit:
                break

            config = OFFER_CONFIGS.get(offer_id)
            if not config: continue

            offer_type = config.get("type")
            was_granted = False

            if offer_type == "GLOBAL_ONLY":
                offer_key = f"offer:{offer_id}:{current_date_str}"
                global_cap = config.get("global_cap")
                result = self.atomic_check_script(keys=[offer_key], args=[global_cap, expire_timestamp])
                if result == "SUCCESS": was_granted = True

            elif offer_type == "USER_ONLY":
                user_key = f"user_offer:{{{user_id}}}:{offer_id}:{current_date_str}"
                user_cap = config.get("user_cap", 1)
                result = self.atomic_check_script(keys=[user_key], args=[user_cap, expire_timestamp])
                if result == "SUCCESS": was_granted = True

            elif offer_type == "BOTH":
                user_key = f"user_offer:{{{user_id}}}:{offer_id}:{current_date_str}"
                user_cap = config.get("user_cap", 1)
                user_result = self.atomic_check_script(keys=[user_key], args=[user_cap, expire_timestamp])
                if user_result == "SUCCESS":
                    offer_key = f"offer:{offer_id}:{current_date_str}"
                    global_cap = config.get("global_cap")
                    global_result = self.atomic_check_script(keys=[offer_key], args=[global_cap, expire_timestamp])
                    if global_result == "SUCCESS": was_granted = True

            if was_granted:
                offers_to_return.append(offer_id)
        print(offers_to_return)
        return offers_to_return

    @task
    def claim_offer_bundle_task(self):
        """
        The main Locust task that simulates a user action, calls the logic,
        and reports the performance results.
        """
        # Get test parameters from the command line
        offers_to_sample_count = self.environment.parsed_options.offers_to_sample
        grant_limit = self.environment.parsed_options.grant_limit

        # Ensure we don't try to sample more offers than exist
        k = min(offers_to_sample_count, len(ALL_OFFER_IDS))

        # Prepare data for this specific task run
        user_id = str(ObjectId())
        applicable_offers = random.sample(ALL_OFFER_IDS, k=k)

        # --- Execute the logic and measure performance ---
        request_name = "python_iterative:claim_offers"
        start_time = time.time()
        try:
            granted_offers = self._claim_offers_iterative_atomic(
                user_id=user_id,
                applicable_offers_ids=applicable_offers,
                grant_limit=grant_limit
            )
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