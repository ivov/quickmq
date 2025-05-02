--[[
Send a message to the queue atomically.

KEYS[1] - Hash key for queue
KEYS[2] - Sorted set key for queue
ARGV[1] - Message ID 
ARGV[2] - Message content
ARGV[3] - Priority

Returns the enqueued_at timestamp
--]]

local hash_key = KEYS[1]
local sorted_set_key = KEYS[2]
local msg_id = ARGV[1]
local msg_content = ARGV[2]
local priority = ARGV[3]

local time = redis.call("TIME")
local seconds = time[1]
local microseconds = time[2]
local now_ms = seconds * 1000 + math.floor(microseconds / 1000)
local member = string.format("%d:%d:%s", priority, now_ms, msg_id)

redis.call("ZADD", sorted_set_key, 0, member)
redis.call("HSET", hash_key, msg_id, msg_content)
redis.call("HSET", hash_key, msg_id .. ":enqueued_at", now_ms)

return now_ms