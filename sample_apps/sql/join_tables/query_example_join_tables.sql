SELECT events.gpio, temperature, humidity, name, status
FROM "amazon-timestream-tools"."sensor_events" as events
JOIN "amazon-timestream-tools"."sensor_details" as details
on events.gpio = details.gpio
where details.status = 'active'