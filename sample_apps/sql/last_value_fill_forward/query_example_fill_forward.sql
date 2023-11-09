with time_seq_only as ( -- Timesequence is 30,240 data points, starting 2 weeks ago of total 3 weeks (1 week in future) with 1 min intervals
    select
    date_add('day', day,
    date_add('hour', hour,
    date_add('second', second, bin('2023-09-20 17:12:22.958000000',1m)))) as time
    from unnest(sequence(0,3540,60)) t(second) cross join unnest (sequence(0, 23)) as t(hour) cross join unnest (sequence(0, 20)) as t(day)
	order by day, hour, second
),
distinct_gpio as ( -- each device identified by gpio needs its own time sequence, this query gets all device ids
    select distinct(gpio) as gpio from "amazon-timestream-tools"."sensordata"
),
time_seq_with_gpio as ( -- multiple time sequences, one for each device identified by gpio
    select time, gpio from time_seq_only join distinct_gpio on true
),
raw_pos as (
SELECT bin(time, 1m) as p_time,
   avg(temperature) as temperature,
   gpio
FROM "amazon-timestream-tools"."sensordata" -- adjust if data is loaded to different table
   where time between '2023-09-20 17:12:22.958000000' and now() -- sample data set contains data from 09/20/2023
   -- and gpio = '22'
GROUP BY gpio, bin(time, 1m)
),
-- dataset contains missing records as just symbol (key), timestamp and all other columns are null
dataset as (
select time_seq_with_gpio.gpio, bin(time, 1m) as time, temperature from time_seq_with_gpio
left join raw_pos
on time_seq_with_gpio.time = raw_pos.p_time
    and time_seq_with_gpio.gpio = raw_pos.gpio
),
filled_set as (
  SELECT
    dataset.gpio
    ,time
    ,temperature as origin_temperature, LAST_VALUE(temperature) IGNORE NULLS OVER (PARTITION BY gpio ORDER BY time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS filled_temperature
    -- Repeat line above fore each columns that should be filled
  FROM
    dataset
  ORDER BY
    gpio,
    time DESC
)
select * from filled_set
-- select * from dataset -- use this line to review original data with containing gaps
order by gpio, time