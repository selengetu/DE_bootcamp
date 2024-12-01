WITH device_activity AS (
    SELECT
        e.user_id,
        d.browser_type,
        TO_DATE(e.event_time, 'YYYY-MM-DD') AS event_date
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
),
-- Generate a date series from the range of active dates (this could be customized based on your range)
series AS (
    SELECT *
    FROM generate_series(
        (SELECT MIN(event_date) FROM device_activity),
        (SELECT MAX(event_date) FROM device_activity),
        INTERVAL '1 day'
    ) AS series_date
),
-- Create a placeholder integer based on the presence of a user on a specific date
place_holder_ints AS (
    SELECT
        da.user_id,
        da.browser_type,
        CASE
            WHEN da.event_date = series.series_date THEN
                CAST(POW(2, 32 - (series.series_date - da.event_date)) AS BIGINT)
            ELSE 0
        END AS placeholder_int_value
    FROM device_activity da
    CROSS JOIN series
)
-- Aggregate the values into a cumulative integer
SELECT
    user_id,
    browser_type,
    CAST(SUM(placeholder_int_value) AS BIGINT) AS datelist_int,
    BIT_COUNT(CAST(SUM(placeholder_int_value) AS BIGINT)) > 0 AS dim_is_active -- checks if there's any activity
FROM place_holder_ints
GROUP BY user_id, browser_type
ORDER BY user_id, browser_type;
