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