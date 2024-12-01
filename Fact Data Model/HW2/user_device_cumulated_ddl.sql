CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_type TEXT,
    device_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (user_id, browser_type, date) );

WITH device_activity AS (
    SELECT
        e.user_id,
        d.browser_type,
        -- Convert event_time to DATE (assuming it's stored as text, adjust as needed)
        TO_DATE(e.event_time, 'YYYY-MM-DD') AS event_date
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
)
-- Aggregate the dates into a cumulative list for each user and browser type
SELECT
    user_id,
    browser_type,
    ARRAY_AGG(DISTINCT event_date ORDER BY event_date) AS device_activity_datelist
FROM device_activity
GROUP BY user_id, browser_type
ORDER BY user_id, browser_type;
