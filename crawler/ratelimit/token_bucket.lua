-- Atomic per-host token bucket.
-- KEYS[1]  = bucket key (e.g. "rl:example.com")
-- ARGV[1]  = capacity (max tokens / burst)
-- ARGV[2]  = refill rate (tokens per second) == 1 / crawl_delay_seconds
-- ARGV[3]  = now (epoch milliseconds)
-- ARGV[4]  = requested tokens (usually 1)
-- ARGV[5]  = key TTL in milliseconds
-- Returns {allowed (1|0), wait_ms} where wait_ms is how long until enough tokens.

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  ts = now_ms
end

local elapsed = math.max(0, now_ms - ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * refill_per_sec)

local allowed = 0
local wait_ms = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  local deficit = requested - tokens
  wait_ms = math.ceil((deficit / refill_per_sec) * 1000)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now_ms)
redis.call('PEXPIRE', key, ttl_ms)

return {allowed, wait_ms}
