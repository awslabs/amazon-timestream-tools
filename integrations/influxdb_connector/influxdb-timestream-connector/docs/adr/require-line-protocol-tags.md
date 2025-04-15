# Requirement of Line Protocol to Include at Least One Tag

## Status

Accepted.

## Context

Ingesting data to Timestream using the AWS SDK for Rust requires building [records](https://docs.aws.amazon.com/timestream/latest/developerguide/API_Record.html). A record includes various fields. Among them is an array of dimensions, "metadata attributes of the time series." All records require at least one dimension.

## Decision

Line protocol uses the following format:
```
<measurement>,<tag key>=<tag value> <field key>=<field value> <timestamp>
```
for example, using a timestamp in seconds:
```
readings,fleet=Alberta velocity=52.13 1725050416
```
All components of a line protocol point are required except for tags.

We have decided that all line protocol points ingested to Timestream using the InfluxDB Timestream connector must have at least one tag. This is due to the fact that we map tags to dimensions.

## Consequences

This means that some tweaking of ingested data is required on the user's side. Users must ensure all line protocol points have at least one tag.
