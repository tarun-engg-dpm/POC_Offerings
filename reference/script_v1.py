import json
import redis
import datetime
from collections import defaultdict

# --- The Lua Script (copied from above) ---
# It's good practice to keep this in a separate .lua file and load it,
# but for this example, we'll define it as a multi-line string.
GRANT_WHAT_IS_AVAILABLE_LUA = """
--[[
  Atomically tries to claim a list of offers for a user and returns a list
  of only the offers that were successfully claimed.
  This version uses EXPIREAT for a coordinated, absolute expiration time.

  Expected Input:
  KEYS table: An interleaved list of keys for the offers to be claimed.
    Format: { offer1_key, user1_key, offer2_key, user2_key, ... }

  ARGV table: An interleaved list of caps, followed by a single UNIX TIMESTAMP.
    Format: { offer1_cap, user1_cap, offer2_cap, user2_cap, ..., expire_at_timestamp }
--]]

local granted_offers = {}
local expire_at_timestamp = tonumber(ARGV[#ARGV])
local num_offers_to_try = #KEYS / 2

for i = 1, num_offers_to_try do
  local offer_key_index = (i * 2) - 1
  local user_key_index = i * 2

  local offer_key = KEYS[offer_key_index]
  local user_key = KEYS[user_key_index]

  local offer_cap = tonumber(ARGV[offer_key_index])
  local user_cap = tonumber(ARGV[user_key_index])

  local offer_count = tonumber(redis.call('GET', offer_key) or 0)
  local user_count = tonumber(redis.call('GET', user_key) or 0)

  if offer_count < offer_cap and user_count < user_cap then
    local new_offer_count = redis.call('INCR', offer_key)
    local new_user_count = redis.call('INCR', user_key)

    table.insert(granted_offers, offer_key)

    if new_offer_count == 1 then
      redis.call('EXPIREAT', offer_key, expire_at_timestamp)
    end
    if new_user_count == 1 then
      redis.call('EXPIREAT', user_key, expire_at_timestamp)
    end
  end
end

return granted_offers
"""


def claim_offers_for_user(redis_client, user_id, offers_to_claim):
    """
    Attempts to claim a bundle of offers for a user using the Lua script.

    Args:
        redis_client: An active redis.Redis client instance.
        user_id: The ID of the user (e.g., a Mongo Object ID string).
        offers_to_claim: A list of dictionaries, where each dict contains
                         'offer_id', 'offer_cap', and 'user_cap'.

    Returns:
        A list of the offer keys that were successfully granted.
    """
    print(f"\n--- Attempting to claim {len(offers_to_claim)} offers for user '{user_id}' ---")

    # --- 1. Register the Lua script with Redis ---
    # This sends the script to Redis once and then uses its SHA hash for
    # subsequent calls, which is more efficient than sending the script every time.
    claim_script = redis_client.register_script(GRANT_WHAT_IS_AVAILABLE_LUA)

    # --- 2. Prepare Keys and Arguments for the script ---
    keys_list = []
    args_list = []
    current_date_str = datetime.datetime.now().strftime("%Y%m%d")

    for offer in offers_to_claim:
        offer_key = f"offer:{offer['offer_id']}:{current_date_str}"
        user_key = f"user_offer:{user_id}:{offer['offer_id']}:{current_date_str}"
        keys_list.extend([offer_key, user_key])
        args_list.extend([offer['offer_cap'], offer['user_cap']])

    # --- 3. Calculate the absolute expiration timestamp for EXPIREAT ---
    # We'll set it to 1:00 AM of the next day to provide a safe grace period.
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
    expire_datetime = tomorrow.replace(hour=1, minute=0, second=0, microsecond=0)
    expire_at_timestamp = int(expire_datetime.timestamp())

    # Add the timestamp as the final argument
    args_list.append(expire_at_timestamp)

    print(f"Keys will be set to expire at: {expire_datetime.isoformat()} (Timestamp: {expire_at_timestamp})")

    # --- 4. Execute the script ---
    # The client library handles passing the keys and args correctly.
    # We call the script object directly.
    try:
        # Note: In redis-py, we don't need to specify the number of keys.
        # The library handles it when using a registered script.
        granted_offer_keys = claim_script(keys=keys_list, args=args_list)
        # The result from redis is a list of bytes, so we decode it.
        granted_offer_keys_str = [key.decode('utf-8') for key in granted_offer_keys]

        print(f"[SUCCESS] The script executed.")
        print(f"Successfully granted {len(granted_offer_keys_str)} out of {len(offers_to_claim)} attempted offers.")
        print("Granted offers:", granted_offer_keys_str)
        return granted_offer_keys_str

    except redis.exceptions.ResponseError as e:
        print(f"[ERROR] The Lua script failed to execute: {e}")
        return []
    except redis.exceptions.ConnectionError as e:
        print(f"[ERROR] Could not connect to Redis: {e}")
        return []


def read_offers_from_json(file_path):
    """
    Reads user offers from a JSON file and organizes them by user_id.

    Args:
        file_path: Path to the JSON file.

    Returns:
        A dictionary where the key is the user_id and the value is a list of offers.
    """
    user_offers = defaultdict(list)

    with open(file_path, mode='r') as json_file:
        data = json.load(json_file)
        for user in data['users']:
            user_id = user['user_id']
            for offer in user['offers']:
                user_offers[user_id].append({
                    "offer_id": offer['offer_id'],
                    "offer_cap": int(offer['offer_cap']),
                    "user_cap": int(offer['user_cap']),
                })

    return user_offers


if __name__ == "__main__":
    # --- Example Usage ---

    # Connect to your local Redis instance.
    # For ElastiCache, you would use the cluster's endpoint URL.
    # NOTE: For a Redis Cluster, you would use redis.cluster.RedisCluster
    redis_host = "coupons-cache.moeinternal.com"
    redis_port = 6379
    r = redis.StrictRedis(host=redis_host, port=redis_port)

    # # Define our sample data
    # sample_user_id = "507f1f77bcf86cd799439011"
    # offers_to_try = [
    #     {"offer_id": "62a24e9334c9d782a2533e4a", "offer_cap": 1000, "user_cap": 1},  # Should succeed
    #     {"offer_id": "62a24e9334c9d782a2533e4b", "offer_cap": 5, "user_cap": 1},  # Let's pretend this one is capped
    #     {"offer_id": "62a24e9334c9d782a2533e4c", "offer_cap": 5000, "user_cap": 1},  # Should succeed
    # ]

    offers_to_try = read_offers_from_json("../resources/offers_db.json")

    # # --- Pre-populate a key to simulate a capped offer ---
    # # This is just for the demonstration to show the script skipping a capped offer.
    # capped_offer_id = "62a24e9334c9d782a2533e4b"
    # date_str = datetime.datetime.now().strftime("%Y%m%d")
    # r.set(f"offer:{capped_offer_id}:{date_str}", 5)  # Set count equal to cap
    # print(f"--- Pre-set cap for offer '{capped_offer_id}' to simulate a breach ---")
    #
    # # Run the main function
    # claim_offers_for_user(redis_client=r, user_id=sample_user_id, offers_to_claim=offers_to_try)

    # Iterate over each user and their offers
    for user_id, offers_to_claim in offers_to_try.items():
        print(f"--- Processing offers for user '{user_id}' ---")
        claim_offers_for_user(redis_client=r, user_id=user_id, offers_to_claim=offers_to_claim)
