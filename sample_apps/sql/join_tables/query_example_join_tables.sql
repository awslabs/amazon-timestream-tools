SELECT events.time, events.gpio, temperature, humidity, name, status
FROM "amazon-timestream-tools"."sensor_events" as events
JOIN "amazon-timestream-tools"."sensor_details" as details
on events.gpio = details.gpio
where details.status = 'active'
and events.time between '2023-09-19' and '2023-11-01'
and details.time between '2023-09-19' and '2023-11-01'

-- also add CTE example