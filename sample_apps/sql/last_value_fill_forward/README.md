# Gap filling with last value

## Query uses multiple steps

1. create time sequence (not limited to just 10,000 data points)
2. select raw data binned at same intervals
3. join time sequence with raw data as data set that contains NULL values now
4. use LAST_VALUE in filled dataset, this query lists justs 2 measures: orignal temperature that can contain NULL and the filled column

The result set shows both original value containing NULL and the filled value in a separate column