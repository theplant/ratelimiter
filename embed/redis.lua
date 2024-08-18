local key = KEYS[1]
local durationPerToken = tonumber(ARGV[1]) -- The time interval required for each token, in microseconds
local burst = tonumber(ARGV[2]) -- Burst capacity
local tokens = tonumber(ARGV[3]) -- Number of tokens requested
local now = tonumber(ARGV[4]) -- Current timestamp, in microseconds
local maxFutureReserve = tonumber(ARGV[5]) -- Maximum reservation duration, in microseconds

if durationPerToken <= 0 or burst <= 0 or tokens <= 0 or tokens > burst then
	return {-2, 0, 0} -- Indicates invalid parameters
end

if now <= 0 then
	local time = redis.call("TIME")
	local time_seconds = tonumber(time[1])
	local time_microseconds = tonumber(time[2])
	now = time_seconds * 1000000 + time_microseconds
end

-- Calculate the reset value based on the current time and burst duration
local resetValue = now - (burst * durationPerToken)

-- Attempt to get timeBase
local timeBase = tonumber(redis.call("get", key))

-- If timeBase does not exist or is less than the calculated reset value, update it to the reset value
if not timeBase or timeBase < resetValue then
	if not timeBase then
		redis.call("set", key, resetValue) 
	end
	timeBase = resetValue
end

-- Calculate the time length occupied by the tokens
local tokensDuration = tokens * durationPerToken
local timeToAct = timeBase + tokensDuration

-- If timeToAct exceeds the maximum reservation timeout, do not update timeBase and return an error
if timeToAct > now + maxFutureReserve then
	return {-1, timeToAct, now} -- Error indicator and returns timeToAct
else
	-- Update timeBase to the execution time of the next request
	redis.call("set", key, timeToAct)
	-- Return the time point when the next action should be performed
	return {0, timeToAct, now} -- Success indicator and returns timeToAct
end