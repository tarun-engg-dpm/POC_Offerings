REDIS_HOST = "dpm-offerings-cache.e6nc9i.clustercfg.aps1.cache.amazonaws.com"
# export REDIS_HOST="dpm-offerings-cache.e6nc9i.clustercfg.aps1.cache.amazonaws.com"
# export OFFERS_JSON_FILE="resources/offers_db.json"
# locust -f one_node_locust.py --offers-to-evaluate 20 --n-to-secure 10 --web-port 8095

import os
import json
import random
import time
import datetime
from bson import ObjectId
import redis
from locust import User, task, between, events

# --- Configuration ---
REDIS_HOST = os.environ.get("REDIS_HOST", "dpm-offerings-cache.e6nc9i.clustercfg.aps1.cache.amazonaws.com")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
OFFERS_JSON_FILE = os.environ.get("OFFERS_JSON_FILE", "resources/both_only.json")

# --- Global Data (Loaded once at test start) ---
OFFER_CONFIGS = {}
ALL_OFFER_IDS = []

# --- "INCR-FIRST" LUA SCRIPTS (minified for performance) ---
ATOMIC_INCR_THEN_CHECK_BOTH_LUA = """
local g=KEYS[1];local u=KEYS[2];local gc=tonumber(ARGV[1]);local uc=tonumber(ARGV[2]);local ex=tonumber(ARGV[3]);local nu=redis.call('INCR',u);if nu<=uc then local ng=redis.call('INCR',g);if ng<=gc then if nu==1 then redis.call('EXPIREAT',u,ex)end;if ng==1 then redis.call('EXPIREAT',g,ex)end;return"SUCCESS"else redis.call('DECR',g);redis.call('DECR',u);return"FAIL_GLOBAL_CAP_MET"end else redis.call('DECR',u);return"FAIL_USER_CAP_MET"end
"""
ATOMIC_INCR_THEN_CHECK_SINGLE_LUA = """
local k=KEYS[1];local c=tonumber(ARGV[1]);local ex=tonumber(ARGV[2]);local n=redis.call('INCR',k);if n<=c then if n==1 then redis.call('EXPIREAT',k,ex)end;return"SUCCESS"else redis.call('DECR',k);return"FAIL_CAP_MET"end
"""


@events.init_command_line_parser.add_listener
def add_custom_arguments(parser):
    """Adds command-line arguments to control the test run."""
    parser.add_argument(
        "--offers-to-evaluate", type=int, env_var="LOCUST_OFFERS_TO_EVALUATE", default=20,
        help="Number of offers to randomly select as the user's applicable pool."
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
    Simulates a user securing 'N' offers using the pipelined "INCR-First" method.
    """
    wait_time = between(0.01, 0.05)  # Short wait time for high throughput testing

    def on_start(self):
        """Called once per user. Connects to Redis and registers scripts."""
        if not ALL_OFFER_IDS:
            self.environment.runner.quit()
            return

        try:
            print(f"Connecting to single Redis node at {REDIS_HOST}:{REDIS_PORT}")
            self.client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
            self.client.ping()
        except Exception as e:
            print(f"Fatal: Could not connect to Redis. Error: {e}")
            self.environment.runner.quit()

        # Register Lua scripts once per user
        self.scripts = {
            'both': self.client.register_script(ATOMIC_INCR_THEN_CHECK_BOTH_LUA),
            'single': self.client.register_script(ATOMIC_INCR_THEN_CHECK_SINGLE_LUA)
        }
        self.expire_at_timestamp = int(
            (datetime.datetime.now() + datetime.timedelta(days=1)).replace(hour=1, minute=0, second=0,
                                                                           microsecond=0).timestamp())

    def _claim_offers_pipelined_incr_first(self, user_id, applicable_offers_ids, n_required):
        """The core orchestration logic being tested."""
        claimed_set = set()
        attempted_set = set()
        max_attempts = n_required + 5
        current_date_str = datetime.datetime.now().strftime("%Y%m%d")

        for _ in range(max_attempts):
            if len(claimed_set) >= n_required: break

            needed_count = n_required - len(claimed_set)
            available_to_try = list(set(applicable_offers_ids) - attempted_set)
            if not available_to_try: break

            batch_to_try = available_to_try[:needed_count]
            attempted_set.update(batch_to_try)

            pipe = self.client.pipeline(transaction=False)
            for offer_id in batch_to_try:
                config = OFFER_CONFIGS.get(offer_id)
                if not config: continue
                offer_type = config.get("type")

                # if offer_type == "GLOBAL_ONLY":
                #     key = f"offer:{offer_id}:{current_date_str}"
                #     self.scripts['single'](keys=[key], args=[config.get("global_cap"), self.expire_at_timestamp],
                #                            client=pipe)
                # elif offer_type == "USER_ONLY":
                #     key = f"user_offer:{user_id}:{offer_id}:{current_date_str}"
                #     self.scripts['single'](keys=[key], args=[config.get("user_cap", 1), self.expire_at_timestamp],
                #                            client=pipe)
                if offer_type == "BOTH":
                    global_key = f"offer:{{{offer_id}}}:{current_date_str}"
                    user_key = f"user_offer:{{{offer_id}}}:{user_id}:{current_date_str}"
                    self.scripts['both'](keys=[global_key, user_key],
                                         args=[config.get("global_cap"), config.get("user_cap", 1),
                                               self.expire_at_timestamp], client=pipe)

            try:
                results = pipe.execute()
                for offer_id, result in zip(batch_to_try, results):
                    if result == b"SUCCESS":
                        claimed_set.add(offer_id)
            except redis.exceptions.RedisError:
                # The failure will be caught and reported by the main task
                raise

        return list(claimed_set)

    @task
    def secure_n_offers_task(self):
        """The main Locust task that simulates the entire user journey."""
        offers_to_evaluate_count = self.environment.parsed_options.offers_to_evaluate
        n_to_secure = self.environment.parsed_options.n_to_secure

        k = min(offers_to_evaluate_count, len(ALL_OFFER_IDS))
        user_id = str(ObjectId())
        applicable_offers = random.sample(ALL_OFFER_IDS, k=k)

        request_name = "orchestrator:pipelined_incr_first"
        start_time = time.time()
        try:
            final_granted_offers = self._claim_offers_pipelined_incr_first(user_id, applicable_offers, n_to_secure)
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