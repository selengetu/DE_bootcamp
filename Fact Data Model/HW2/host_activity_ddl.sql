CREATE TABLE host_activity_reduced (
    user_id TEXT,              -- The user associated with the host
    host TEXT,                 -- The host (IP address or hostname)
    month_start DATE,          -- The start of the month for the activity
    total_site_hits INT,       -- The total number of site hits in the month
    PRIMARY KEY (user_id, host, month_start)  -- Primary key to ensure uniqueness
);
-- Step 1: Aggregate the events data by user, host, and month
WITH monthly_activity AS (
    SELECT
        e.user_id,
        e.host,
        DATE_TRUNC('month', TO_DATE(e.event_time, 'YYYY-MM-DD')) AS month_start,
        COUNT(1) AS total_site_hits  -- Calculate total site hits in the month
    FROM events e
    WHERE e.event_time IS NOT NULL
    GROUP BY e.user_id, e.host, month_start
)
-- Step 2: Insert or update the aggregated data in the host_activity_reduced table
INSERT INTO host_activity_reduced (user_id, host, month_start, total_site_hits)
SELECT
    ma.user_id,
    ma.host,
    ma.month_start,
    ma.total_site_hits
FROM monthly_activity ma
ON CONFLICT (user_id, host, month_start)  -- Handle conflicts (duplicates)
DO UPDATE SET
    total_site_hits = EXCLUDED.total_site_hits;  -- Update the total_site_hits if there's a conflict
