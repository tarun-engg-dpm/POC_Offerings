import redis
import datetime
import time
from pprint import pprint

# ==============================================================================
#  1. LUA SCRIPTS
# ==============================================================================

# Script for offers with only a global cap.
# Atomically checks the cap and increments if it's not met.
GLOBAL_ONLY_LUA_SCRIPT = """
    -- KEYS[1]: The global offer key, e.g., "offer:offerB:20250611"
    -- ARGV[1]: The overall offer cap
    -- ARGV[2]: The absolute expire_at_timestamp
    local offer_key = KEYS[1]
    local offer_cap = tonumber(ARGV[1])
    local expire_at_timestamp = tonumber(ARGV[2])

    if (redis.call('GET', offer_key) or 0) < offer_cap then
      local new_val = redis.call('INCR', offer_key)
      if new_val == 1 then
        redis.call('EXPIREAT', offer_key, expire_at_timestamp)
      end
      return "SUCCESS"
    else
      return "FAIL_CAP_MET"
    end
"""

# Script for offers with only a user-level cap. This is the most scalable pattern.
# The key MUST use a hash tag on the user_id for this to work in a cluster.
USER_ONLY_LUA_SCRIPT = """
    -- KEYS[1]: The user-specific key using a hash tag, e.g., "user_offer:{userX}:offerC:20250611"
    -- ARGV[1]: The user-level cap
    -- ARGV[2]: The absolute expire_at_timestamp
    local user_key = KEYS[1]
    local user_cap = tonumber(ARGV[1])
    local expire_at_timestamp = tonumber(ARGV[2])

    local user_count = tonumber(redis.call('GET', user_key) or 0)

    if user_count < user_cap then
      local new_val = redis.call('INCR', user_key)
      -- Note: EXPIREAT is only set on the first INCR to be efficient
      if new_val == 1 then
          redis.call('EXPIREAT', user_key, expire_at_timestamp)
      end
      return "SUCCESS"
    else
      return "FAIL_USER_CAP_MET"
    end
"""

# ==============================================================================
#  2. OFFER CONFIGURATION
# ==============================================================================
# In a real application, this would be loaded from a config file or database.
OFFER_CONFIGS = {
    "offer_A_both": {
        "type": "BOTH",
        "global_cap": 100000,
        "user_cap": 1
    },
    "offer_B_global": {
        "type": "GLOBAL_ONLY",
        "global_cap": 500
    },
    "offer_C_user": {
        "type": "USER_ONLY",
        "user_cap": 5
    },
    "offer_D_capped": {
        "type": "BOTH",
        "global_cap": 10,
        "user_cap": 1
    }
}


# ==============================================================================
#  3. MAIN APPLICATION LOGIC
# ==============================================================================

def claim_offers_flexible(redis_client, user_id, offers_to_try):
    """
    Claims a bundle of offers using a flexible, configuration-driven, and
    pipelined approach suitable for a Redis Cluster.

    Args:
        redis_client: An active redis.Redis client instance.
        user_id: The ID of the user.
        offers_to_try: A list of offer_id strings to attempt to claim.
    """
    print(f"\n{'=' * 50}\nAttempting to claim {len(offers_to_try)} offers for user '{user_id}'...\n{'=' * 50}")

    # --- Pre-computation ---
    current_date_str = datetime.datetime.now().strftime("%Y%m%d")
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
    expire_datetime = tomorrow.replace(hour=1, minute=0, second=0, microsecond=0)
    expire_at_timestamp = int(expire_datetime.timestamp())
    print(f"All new keys will be set to expire at: {expire_datetime.isoformat()}")

    # --- Register Lua Scripts for efficiency ---
    global_script = redis_client.register_script(GLOBAL_ONLY_LUA_SCRIPT)
    user_script = redis_client.register_script(USER_ONLY_LUA_SCRIPT)

    # This list will track offers that pass the user-level check and need a global INCR
    offers_for_global_incr = []
    # This list will track the final results
    results_summary = {}

    # --- Pipeline 1: Handle all User-Level and Global-Only checks ---
    # This pipeline runs the atomic checks. For 'BOTH' type, it only does the user-level part.
    pipe = redis_client.pipeline(transaction=False)
    print("\n--- Pipeline 1: Executing User-Level and Global-Only Atomic Checks ---")

    for offer_id in offers_to_try:
        config = OFFER_CONFIGS.get(offer_id)
        if not config:
            print(f"Warning: No configuration found for '{offer_id}'. Skipping.")
            continue

        cap_type = config['type']

        if cap_type == 'GLOBAL_ONLY':
            key = f"offer:{offer_id}:{current_date_str}"
            args = [config['global_cap'], expire_at_timestamp]
            global_script(keys=[key], args=args, client=pipe)

        elif cap_type == 'USER_ONLY':
            # Use hash tags for even distribution in a cluster
            key = f"user_offer:{{{user_id}}}:{offer_id}:{current_date_str}"
            args = [config['user_cap'], expire_at_timestamp]
            user_script(keys=[key], args=args, client=pipe)

        elif cap_type == 'BOTH':
            # In the first pass, we only do the critical user-level check
            key = f"user_offer:{{{user_id}}}:{offer_id}:{current_date_str}"
            args = [config['user_cap'], expire_at_timestamp]
            user_script(keys=[key], args=args, client=pipe)

    # Execute the first pipeline
    pipeline1_results = pipe.execute()
    print("Pipeline 1 Results:", pipeline1_results)

    # --- Process results of Pipeline 1 and prepare Pipeline 2 ---
    for i, offer_id in enumerate(offers_to_try):
        config = OFFER_CONFIGS.get(offer_id)
        result = pipeline1_results[i]

        if result == b'SUCCESS':
            if config['type'] == 'BOTH':
                # If the user-level check for a 'BOTH' offer passed,
                # we now need to increment its global counter.
                offers_for_global_incr.append(offer_id)
            results_summary[offer_id] = 'SUCCESS'
        else:
            results_summary[offer_id] = 'FAIL'

    # --- Pipeline 2: Handle Best-Effort Global Increments for 'BOTH' types ---
    if offers_for_global_incr:
        print("\n--- Pipeline 2: Executing Best-Effort Global Increments for 'BOTH' type offers ---")
        pipe2 = redis_client.pipeline(transaction=False)
        for offer_id in offers_for_global_incr:
            global_key = f"offer:{offer_id}:{current_date_str}"
            pipe2.incr(global_key)
            # We don't need the result, but executing the pipeline sends the commands

        pipe2.execute()
        print("Pipeline 2 complete.")
    else:
        print("\n--- Pipeline 2: No global increments needed ---")

    # --- Final Report ---
    print(f"\n--- FINAL RESULTS for user '{user_id}' ---")
    pprint(results_summary)

    successfully_granted = [offer_id for offer_id, status in results_summary.items() if status == 'SUCCESS']
    return successfully_granted


# ==============================================================================
#  4. EXAMPLE EXECUTION
# ==============================================================================
if __name__ == "__main__":
    try:
        # For ElastiCache Cluster, use redis.cluster.RedisCluster and the config endpoint
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
        # Ping to ensure connection is alive
        r.ping()
        print("Successfully connected to Redis.")
    except redis.exceptions.ConnectionError as e:
        print(f"Could not connect to Redis. Please ensure it's running. Error: {e}")
        exit()

    # --- Setup for Demonstration ---
    sample_user_id = "user-a4b8-91c3"
    # Try to claim all four configured offers
    offers_to_attempt = ["offer_A_both", "offer_B_global", "offer_C_user", "offer_D_capped"]

    # Pre-cap one of the offers to demonstrate a failure
    date_str = datetime.datetime.now().strftime("%Y%m%d")
    capped_offer_key = f"offer:offer_D_capped:{date_str}"
    r.set(capped_offer_key, 10)  # Set count equal to its cap of 10
    print(f"\nSETUP: Pre-setting cap for '{capped_offer_key}' to demonstrate a failure.")

    # --- Run the main logic ---
    final_granted_offers = claim_offers_flexible(r, sample_user_id, offers_to_attempt)

    # --- Cleanup (optional, but good for testing) ---
    print("\n--- Cleanup: Deleting demo keys ---")
    keys_to_delete = [
        f"offer:offer_A_both:{date_str}",
        f"user_offer:{{{sample_user_id}}}:offer_A_both:{date_str}",
        f"offer:offer_B_global:{date_str}",
        f"user_offer:{{{sample_user_id}}}:offer_C_user:{date_str}",
        capped_offer_key  # The key we pre-set
    ]
    if keys_to_delete:
        deleted_count = r.delete(*keys_to_delete)
        print(f"Deleted {deleted_count} keys.")