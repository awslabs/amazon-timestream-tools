-- Example 1 - Standard JOIN
SELECT events.time, events.gpio, temperature, humidity, name, status
FROM "amazon-timestream-tools"."sensor_events" as events
JOIN "amazon-timestream-tools"."sensor_details" as details
ON events.gpio = details.gpio
WHERE details.status = 'active'
AND events.time BETWEEN '2023-09-19' AND '2023-11-01'
AND details.time BETWEEN '2023-09-19' AND '2023-11-01'

-- Example 2 - CTO JOIN (preferred)
with events as (
    SELECT time, gpio, temperature, humidity
    FROM "amazon-timestream-tools"."sensor_events"
    WHERE time BETWEEN '2023-09-19' AND '2023-11-01'
),
details as (
    SELECT time, gpio, name, status
    FROM "amazon-timestream-tools"."sensor_details"
    WHERE time BETWEEN '2023-09-19' AND '2023-11-01'
    AND status = 'active'
)
SELECT events.time, events.gpio, temperature, humidity, name, status
FROM events
JOIN details
ON events.gpio = details.gpio