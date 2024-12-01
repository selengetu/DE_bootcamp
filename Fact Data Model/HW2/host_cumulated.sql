CREATE TABLE hosts_cumulated (
    host TEXT,               -- The host (e.g., IP address or hostname)
    user_id TEXT,            -- The user associated with the host
    device_id NUMERIC,       -- The device used by the user
    date DATE,               -- The date of the record
    PRIMARY KEY (host, user_id, device_id, date)  -- Primary key based on host, user_id, device_id, and date
);

-- Step 1: Define the `host_activity_datelist` and insert or update the data incrementally
INSERT INTO array_metrics
WITH daily_aggregate AS (
    -- Aggregate daily activity for each user and host
    SELECT
        user_id,
        host,
        DATE(event_time) AS date,
        COUNT(1) AS num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-02')  -- You can parameterize this date
    AND user_id IS NOT NULL
    GROUP BY user_id, host, DATE(event_time)
),
yesterday_array AS (
    -- Get the previous day's activity array (for each user)
    SELECT *
    FROM array_metrics
    WHERE month_start = DATE('2023-01-01')  -- You can parameterize this date
)
-- Step 2: Update the `host_activity_datelist` array with new daily data
SELECT
    COALESCE(da.user_id, ya.user_id) AS user_id,
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    'host_activity' AS metric_name,
    CASE WHEN ya.metric_array IS NOT NULL THEN
        ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]  -- Add today's hits
    WHEN ya.metric_array IS NULL THEN
        ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(da.num_site_hits, 0)]
    END AS metric_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya
    ON da.user_id = ya.user_id
ON CONFLICT(user_id, month_start, metric_name)
DO
    UPDATE SET metric_array = EXCLUDED.metric_array;

-- Step 3: Summarize the `host_activity_datelist` array
WITH agg AS (
    -- Aggregate the daily values to summarize the activity for each user
    SELECT
        metric_name,
        month_start,
        ARRAY[SUM(metric_array[1]), SUM(metric_array[2]), SUM(metric_array[3])] AS summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
-- Step 4: Unnest the summed array for final output
SELECT
    metric_name,
    month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) AS activity_date,
    elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index);
