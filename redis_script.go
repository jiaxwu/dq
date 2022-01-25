package dq

const delayQueuePushRedisScript = `
-- KEYS[1]: topicZSet
-- KEYS[2]: topicHash
-- ARGV[1]: 消息的Key
-- ARGV[2]: 消息的Body
-- ARGV[3]: 消息准备好执行的时间（now + delay）

local topicZSet = KEYS[1]
local topicHash = KEYS[2]
local key = ARGV[1]
local body = ARGV[2]
local readyTime = tonumber(ARGV[3])

-- 添加readyTime到zset
local count = redis.call("zadd", topicZSet, readyTime, key)
-- 消息已经存在
if count == 0 then 
   return 0
end
-- 添加body到hash
redis.call("hsetnx", topicHash, key, body)
return 1
`

const delayQueueDelRedisScript = `
-- KEYS[1]: topicZSet
-- KEYS[2]: topicHash
-- ARGV[1]: 消息的Key

local topicZSet = KEYS[1]
local topicHash = KEYS[2]
local key = ARGV[1]

-- 删除zset和hash关于这条消息的内容
redis.call("zrem", topicZSet, key)
redis.call("hdel", topicHash, key)
return 1
`
