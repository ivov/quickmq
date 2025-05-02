--[[
Remove a message by ID, either 
- by consumer after processing (with ownership verification), or
- by producer before delivery (without ownership verification).

KEYS[1] - Hash key for queue
KEYS[2] - Sorted set key for queue
ARGV[1] - Message ID to remove
ARGV[2] - Lease expiration timestamp (optional, use "0" if not checking ownership)

Returns:
  1 if message was deleted
  0 if message was not found or if ownership check failed
--]]

local hash_key = KEYS[1]
local sorted_set_key = KEYS[2]
local msg_id = ARGV[1]
local lease_expiration_ts = ARGV[2]
local must_check_ownership = lease_expiration_ts ~= "0"

local members = redis.call("ZRANGEBYLEX", sorted_set_key, "-", "+")
local found_member = nil

for _, m in ipairs(members) do
  if string.match(m, ":" .. msg_id .. "$") then
    if must_check_ownership then
      local _, visibility_ts, _ = string.match(m, "(%d):(%d+):(.+)")
      if tonumber(visibility_ts) ~= tonumber(lease_expiration_ts) then
        return 0 -- mismatch, cannot delete unowned message
      end
    end
    found_member = m
    break
  end
end

if not found_member then
  return 0
end

redis.call("HDEL", hash_key, msg_id, msg_id .. ":enqueued_at", msg_id .. ":receive_count")
redis.call("ZREM", sorted_set_key, found_member)

return 1