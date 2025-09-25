--[[
  Functions to collect metrics based on a current and previous count of jobs.
  Granualarity is fixed at 1 minute.
  Also collects execution time metrics when provided.
]]
--- @include "batches"
local function collectMetrics(metaKey, dataPointsList, maxDataPoints,
                                 timestamp, executionTime, collectTimings, timingBucketSeconds)
    -- Increment current count
    local count = rcall("HINCRBY", metaKey, "count", 1) - 1

    -- Compute how many data points we need to add to the list, N.
    local prevTS = rcall("HGET", metaKey, "prevTS")

    if not prevTS then
        -- If prevTS is nil, set it to the current timestamp
        rcall("HSET", metaKey, "prevTS", timestamp, "prevCount", 0)
        
        -- Store execution time even on first run
        if collectTimings == "1" and executionTime and tonumber(executionTime) and tonumber(executionTime) > 0 then
            local bucketSizeMs = (tonumber(timingBucketSeconds) or 15) * 1000
            local bucketTimestamp = math.floor(timestamp / bucketSizeMs) * bucketSizeMs
            -- Use base metrics key without :completed or :failed suffix for unified storage
            local baseKey = string.gsub(metaKey, ":completed$", ""):gsub(":failed$", "")
            local timingKey = baseKey .. ":timings:" .. bucketTimestamp
            -- Store execution time in list
            rcall("LPUSH", timingKey, tonumber(executionTime))
            -- TTL = bucketTime * 10 for safety
            local ttlSeconds = (tonumber(timingBucketSeconds) or 15) * 10
            rcall("EXPIRE", timingKey, ttlSeconds)
        end
        return
    end

    local N = math.min(math.floor(timestamp / 60000) - math.floor(prevTS / 60000), tonumber(maxDataPoints))

    if N > 0 then
        local delta = count - rcall("HGET", metaKey, "prevCount")
        -- If N > 1, add N-1 zeros to the list
        if N > 1 then
            local points = {}
            points[1] = delta
            for i = 2, N do
                points[i] = 0
            end

            for from, to in batches(#points, 7000) do
                rcall("LPUSH", dataPointsList, unpack(points, from, to))
            end
        else
            -- LPUSH delta to the list
            rcall("LPUSH", dataPointsList, delta)
        end

        -- LTRIM to keep list to its max size
        rcall("LTRIM", dataPointsList, 0, maxDataPoints - 1)

        -- update prev count with current count
        rcall("HSET", metaKey, "prevCount", count, "prevTS", timestamp)
    end
    
    -- Store execution time in time-bucketed key if timing collection is enabled
    if collectTimings == "1" and executionTime and tonumber(executionTime) and tonumber(executionTime) > 0 then
        local bucketSizeMs = (tonumber(timingBucketSeconds) or 15) * 1000
        local bucketTimestamp = math.floor(timestamp / bucketSizeMs) * bucketSizeMs
        -- Use base metrics key without :completed or :failed suffix for unified storage
        local baseKey = string.gsub(metaKey, ":completed$", ""):gsub(":failed$", "")
        local timingKey = baseKey .. ":timings:" .. bucketTimestamp
        -- Store execution time in list
        rcall("LPUSH", timingKey, tonumber(executionTime))
        -- TTL = bucketTime * 10 for safety
        local ttlSeconds = (tonumber(timingBucketSeconds) or 15) * 10
        rcall("EXPIRE", timingKey, ttlSeconds)
    end
end
