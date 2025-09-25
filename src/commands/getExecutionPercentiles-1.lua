--[[
  Calculate execution time percentiles from time-bucketed keys
  
  Input:
    KEYS[1] metrics key prefix (e.g., "bull:myqueue:metrics")
    
    ARGV[1] timeWindowSeconds - how many seconds back to look
    ARGV[2] currentTimestamp - current timestamp in milliseconds
    ARGV[3] bucketSizeSeconds - bucket size in seconds
    
  Output:
    [p50, p95, p99, sampleCount]
    
  Example:
    EVAL script 1 "bull:myqueue:metrics" 900 1640995200000 15
]]

local rcall = redis.call
local metricsKeyPrefix = KEYS[1]
local timeWindowSeconds = tonumber(ARGV[1]) or 15  -- Default 15 seconds, so just 1 bucket, but if you query less often, this can be set higher to query more data
local currentTimestamp = tonumber(ARGV[2])
local bucketSizeSeconds = tonumber(ARGV[3]) or 15

-- Collect all execution times from time buckets
local allTimings = {}
local totalSamples = 0

local bucketSizeMs = bucketSizeSeconds * 1000
local numBuckets = math.floor(timeWindowSeconds / bucketSizeSeconds)

for i = 0, numBuckets - 1 do
    local bucketTime = math.floor((currentTimestamp - (i * bucketSizeMs)) / bucketSizeMs) * bucketSizeMs
    local timingKey = metricsKeyPrefix .. ":timings:" .. bucketTime
    
    -- Get all execution times from this bucket (stored as list values)
    local bucketTimings = rcall("LRANGE", timingKey, 0, -1)
    
    -- Add all timing values to our collection
    for j = 1, #bucketTimings do
        local executionTime = tonumber(bucketTimings[j])
        if executionTime then
            table.insert(allTimings, executionTime)
            totalSamples = totalSamples + 1
        end
    end
end

-- Return zeros if no data
if totalSamples == 0 then
    return {0, 0, 0, 0}
end

-- Sort the timings array
table.sort(allTimings)

-- Calculate percentiles
local function calculatePercentile(sortedArray, percentile)
    local index = math.ceil((percentile / 100) * #sortedArray)
    -- Ensure index is within bounds
    index = math.max(1, math.min(index, #sortedArray))
    return sortedArray[index]
end

local p50 = calculatePercentile(allTimings, 50)
local p95 = calculatePercentile(allTimings, 95)
local p99 = calculatePercentile(allTimings, 99)

return {p50, p95, p99, totalSamples}
