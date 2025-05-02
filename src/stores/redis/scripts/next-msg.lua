--[[
Return the next available message, making it invisible. 

KEYS[1] - Hash key for queue
ARGV[1] - Visibility timeout (ms)

Returns one of  
  - Empty array if no message available 
  - Array holding message ID, message content, enqueued timestamp, visibility timestamp, receive count
--]]

local hash_key = KEYS[1]
local sorted_set_key = hash_key .. ":sequence"

-- 1. retrieve next available message (if visibility timestamp in past or present)

local now_ts = redis.call("TIME")[1] * 1000 + math.floor(redis.call("TIME")[2] / 1000)
local members = redis.call("ZRANGEBYLEX", sorted_set_key, "-", "+")
local member = nil

for _, m in ipairs(members) do
  local _, visibility_ts, _ = string.match(m, "(%d):(%d+):(.+)")
  if tonumber(visibility_ts) <= now_ts then
    member = m
    break
  end
end

if not member then
  return {}
end

-- 2. update message's visibility timestamp to future, making it invisible

local visibility_timeout = tonumber(ARGV[1])
local new_visibility_ts = now_ts + visibility_timeout
local priority, _, msg_id = string.match(member, "(%d):(%d+):(.+)")
local new_member = string.format("%d:%d:%s", priority, new_visibility_ts, msg_id)
redis.call("ZREM", sorted_set_key, member)
redis.call("ZADD", sorted_set_key, 0, new_member)

-- 3. update message's receive count

local receive_count = redis.call("HINCRBY", hash_key, msg_id .. ":receive_count", 1)

-- 4. return message

local hash_values = redis.call("HMGET", hash_key, msg_id, msg_id .. ":enqueued_at")
local msg_content = hash_values[1]
local enqueued_at = hash_values[2]

return { msg_id, msg_content, enqueued_at, new_visibility_ts, receive_count }
