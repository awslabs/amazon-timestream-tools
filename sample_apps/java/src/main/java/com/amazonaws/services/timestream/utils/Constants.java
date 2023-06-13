package com.amazonaws.services.timestream.utils;

public class Constants {
    public static final String DATABASE_NAME = "devops";
    public static final String TABLE_NAME = "host_metrics";
    public static final String UNLOAD_TABLE_NAME = "unload_click_stream_data";
    public static final long HT_TTL_HOURS = 24L;
    public static final long CT_TTL_DAYS = 7L;
    public static final String S3_BUCKET_PREFIX = "timestream-sample";
}
