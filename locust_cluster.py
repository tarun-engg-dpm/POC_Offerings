import os
import json
import random
import time
import datetime
from bson import ObjectId
import redis
# --- THIS IS THE NEW IMPORT ---
# For a sharded Redis Cluster, we use the RedisCluster client
from redis.cluster import RedisCluster, ClusterNode
from locust import User, task, between, events

# --- Configuration ---
# Set REDIS_HOST to the endpoint of ONE of your cluster's nodes.
# The client will discover the rest automatically.
REDIS_HOST = os.environ.get("REDIS_HOST", "dpm-offerings-clustered.e6nc9i.clustercfg.aps1.cache.amazonaws.com")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
OFFERS_JSON_FILE = os.environ.get("OFFERS_JSON_FILE", "resources/offers_db.json")

# --- Global variables, loaded once at the start of the test ---
OFFER_CONFIGS = {}
ALL_OFFER_IDS = []


def get_next_day_midnight_timestamp():
    """Calculates the UNIX timestamp for 00:00:00 UTC of the next day."""
    tomorrow = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
    return int(tomorrow.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())


@events.init_command_line_parser.add_listener
def add_custom_arguments(parser):
    """Adds custom command-line arguments to control the test run."""
    parser.add_argument(
        "--offers-to-sample", type=int, env_var="LOCUST_OFFERS_TO_SAMPLE", default=10,
        help="Number of offers to randomly select for each task."
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
            print(f"Error: The offers file '{OFFERS_JSON_FILE}' is empty.")
            environment.runner.quit()
        else:
            print(f"Successfully loaded {len(ALL_OFFER_IDS)} offer configurations.")
    except Exception as e:
        print(f"Fatal error loading configuration file: {e}")
        environment.runner.quit()


class RedisUser(User):
    """
    Simulates a user claiming offers using the one-by-one, cluster-safe method.
    """
    wait_time = between(0.05, 0.2)

    def on_start(self):
        """Called once per user. Connects to the Redis Cluster."""
        if not ALL_OFFER_IDS:
            self.environment.runner.quit()
            return

        try:
            # --- THIS IS THE CRITICAL FIX ---
            # Instead of a simple client, we instantiate a cluster-aware client.
            print(f"Connecting to Redis Cluster with startup node {REDIS_HOST}:{REDIS_PORT}")
            startup_nodes = [ClusterNode(REDIS_HOST, REDIS_PORT)]
            self.client = RedisCluster(
                startup_nodes=startup_nodes,
                decode_responses=True,
                skip_full_coverage_check=True  # Recommended for robust startup in test environments
            )

            # This ping will be intelligently routed to a node in the cluster.
            self.client.ping()
            print("Successfully connected to Redis Cluster.")
        except Exception as e:
            # Catch a broader range of cluster connection errors
            print(f"Fatal: Could not connect to Redis Cluster. Error: {e}")
            self.environment.runner.quit()

    def _claim_offers_one_by_one(self, user_id, applicable_offers_ids, grant_limit):
        """
        The application logic remains UNCHANGED. The cluster client handles all routing.
        """
        offers_to_return = []
        current_date_str = datetime.datetime.now().strftime("%Y%m%d")
        expire_timestamp = get_next_day_midnight_timestamp()

        for offer_id in applicable_offers_ids:
            if len(offers_to_return) >= grant_limit: break

            config = OFFER_CONFIGS.get(offer_id)
            if not config: continue

            offer_type = config.get("type")
            was_granted = False

            if offer_type == "GLOBAL_ONLY":
                key = f"offer:{offer_id}:{current_date_str}"
                if int(self.client.get(key) or 0) < config.get("global_cap"):
                    self.client.incr(key)
                    self.client.expireat(key, expire_timestamp)
                    was_granted = True

            elif offer_type == "USER_ONLY":
                key = f"user_offer:{{{user_id}}}:{offer_id}:{current_date_str}"
                if int(self.client.get(key) or 0) < config.get("user_cap", 1):
                    self.client.incr(key)
                    self.client.expireat(key, expire_timestamp)
                    was_granted = True

            elif offer_type == "BOTH":
                user_key = f"user_offer:{{{user_id}}}:{offer_id}:{current_date_str}"
                if int(self.client.get(user_key) or 0) < config.get("user_cap", 1):
                    offer_key = f"offer:{offer_id}:{current_date_str}"
                    if int(self.client.get(offer_key) or 0) < config.get("global_cap"):
                        self.client.incr(user_key)
                        self.client.expireat(user_key, expire_timestamp)
                        self.client.incr(offer_key)
                        self.client.expireat(offer_key, expire_timestamp)
                        was_granted = True

            if was_granted:
                offers_to_return.append(offer_id)
        print(offers_to_return)
        return offers_to_return

    @task
    def claim_offer_bundle_task(self):
        """
        The main Locust task remains unchanged.
        """
        offers_to_sample_count = self.environment.parsed_options.offers_to_sample
        grant_limit = self.environment.parsed_options.grant_limit

        k = min(offers_to_sample_count, len(ALL_OFFER_IDS))

        user_id = str(ObjectId())
        applicable_offers = random.sample(ALL_OFFER_IDS, k=k)

        request_name = "python_one_by_one:claim_offers"
        start_time = time.time()
        try:
            granted_offers = self._claim_offers_one_by_one(user_id, applicable_offers, grant_limit)
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
            # locust -f locust_cluster.py --offers-to-sample 10 --grant-limit 3 --web-port 8089