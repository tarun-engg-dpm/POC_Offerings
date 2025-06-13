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

# --- LUA SCRIPTS (Our atomic building blocks) ---
GLOBAL_ONLY_LUA_SCRIPT = """
    if (tonumber(redis.call('GET', KEYS[1]) or 0)) < tonumber(ARGV[1]) then
      if redis.call('INCR', KEYS[1]) == 1 then redis.call('EXPIREAT', KEYS[1], tonumber(ARGV[2])) end
      return "SUCCESS"
    else return "FAIL_CAP_MET" end
"""
USER_ONLY_LUA_SCRIPT = """
    if (tonumber(redis.call('GET', KEYS[1]) or 0)) < tonumber(ARGV[1]) then
      if redis.call('INCR', KEYS[1]) == 1 then redis.call('EXPIREAT', KEYS[1], tonumber(ARGV[2])) end
      return "SUCCESS"
    else return "FAIL_USER_CAP_MET" end
"""


@events.init_command_line_parser.add_listener
def add_custom_arguments(parser):
    """Adds command-line arguments to control the test run."""
    parser.add_argument(
        "--offers-to-evaluate", type=int, env_var="LOCUST_OFFERS_TO_EVALUATE", default=10,
        help="Number of offers to randomly select as the user's eligible pool."
    )
    parser.add_argument(
        "--n-to-secure", type=int, env_var="LOCUST_N_TO_SECURE", default=3,
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
    Simulates a user securing 'N' offers using the cluster-safe, iterative-atomic method.
    """
    wait_time = between(0.05, 0.1)  # Wait 100-500ms between tasks

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
                decode_responses=False,  # Lua scripts return bytes
                skip_full_coverage_check=True
            )
            self.client.ping()
            print("Successfully connected to Redis Cluster.")
        except Exception as e:
            print(f"Fatal: Could not connect to Redis Cluster. Error: {e}")
            self.environment.runner.quit()

        # Register the Lua scripts once per user for efficiency
        self.scripts = {
            'global': self.client.register_script(GLOBAL_ONLY_LUA_SCRIPT),
            'user': self.client.register_script(USER_ONLY_LUA_SCRIPT)
        }
        # Pre-calculate the expiration timestamp once per user start
        self.expire_at_timestamp = int(
            (datetime.datetime.now() + datetime.timedelta(days=1)).replace(hour=1, minute=0, second=0,
                                                                           microsecond=0).timestamp())

    def _claim_selected_offers_cluster_safe(self, user_id, offers_to_claim):
        """The low-level, cluster-safe claim function."""
        granted_in_this_batch = []
        date_str = datetime.datetime.now().strftime('%Y%m%d')

        for offer_id in offers_to_claim:
            config = OFFER_CONFIGS.get(offer_id)
            if not config: continue

            cap_type = config.get('type')
            was_granted = False

            try:
                # if cap_type == 'GLOBAL_ONLY':
                #     key = f"offer:{offer_id}:{date_str}"
                #     args = [config['global_cap'], self.expire_at_timestamp]
                #     if self.scripts['global'](keys=[key], args=args) == b'SUCCESS':
                #         was_granted = True
                #
                # elif cap_type == 'USER_ONLY':
                #     key = f"user_offer:{{{user_id}}}:{offer_id}:{date_str}"
                #     args = [config['user_cap'], self.expire_at_timestamp]
                #     if self.scripts['user'](keys=[key], args=args) == b'SUCCESS':
                #         was_granted = True

                if cap_type == 'BOTH':
                    user_key = f"user_offer:{{{user_id}}}:{offer_id}:{date_str}"
                    user_args = [config['user_cap'], self.expire_at_timestamp]
                    if self.scripts['user'](keys=[user_key], args=user_args) == b'SUCCESS':
                        global_key = f"offer:{offer_id}:{date_str}"
                        self.client.incr(global_key)
                        was_granted = True

                if was_granted:
                    granted_in_this_batch.append(offer_id)

            except redis.exceptions.RedisError as e:
                # In a load test, we typically don't print every error, just report it
                pass  # The failure will be caught and reported by the main task
        # print(granted_in_this_batch)
        return granted_in_this_batch

    def _secure_n_offers(self, user_id, offers_to_evaluate, n_required):
        """The high-level orchestration logic being tested."""
        eligible_pool = set(offers_to_evaluate)
        claimed_set = set()
        attempted_set = set()
        max_attempts = 3

        for attempt in range(max_attempts):
            if len(claimed_set) >= n_required: break

            needed_count = n_required - len(claimed_set)
            available_to_try = list(eligible_pool - attempted_set)
            if not available_to_try: break

            offers_to_claim_now = available_to_try[:needed_count]
            attempted_set.update(offers_to_claim_now)

            newly_claimed = self._claim_selected_offers_cluster_safe(user_id, offers_to_claim_now)
            if newly_claimed:
                claimed_set.update(newly_claimed)

        return list(claimed_set)

    @task
    def secure_n_offers_task(self):
        """The main Locust task that simulates the entire user journey."""
        # Get test parameters from the command line
        offers_to_evaluate_count = self.environment.parsed_options.offers_to_evaluate
        n_to_secure = self.environment.parsed_options.n_to_secure

        # Prepare data for this specific task run
        user_id = str(ObjectId())
        # Randomly select a pool of offers for this user to try
        offers_to_evaluate = random.sample(ALL_OFFER_IDS, k=offers_to_evaluate_count)

        # --- Execute the entire orchestration and measure its performance ---
        request_name = "orchestrator:secure_n_offers"
        start_time = time.time()
        try:
            final_granted_offers = self._secure_n_offers(user_id, offers_to_evaluate, n_to_secure)
            total_time = int((time.time() - start_time) * 1000)

            # If we didn't get the required number, we can optionally mark it as a failure
            if len(final_granted_offers) < n_to_secure:
                events.request.fire(
                    request_type="redis", name=request_name, response_time=total_time,
                    response_length=len(str(final_granted_offers)), exception=f"Failed to secure {n_to_secure} offers",
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
            # locust -f locust_v3.py --offers-to-evaluate 15 --n-to-secure 4