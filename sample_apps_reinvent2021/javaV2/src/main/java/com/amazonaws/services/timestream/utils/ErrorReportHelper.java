package com.amazonaws.services.timestream.utils;

import java.util.List;


import lombok.experimental.UtilityClass;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Class that parses ErrorReport from S3. A sample error report looks like:
 * {"reportId":"8A729E3CA0755E13EFE4346FB24875AA",
 * "errors":[{"reason":"The record timestamp is outside the time range [2021-11-19T00:07:04.593Z, 2021-11-20T00:47:04.593Z) of the memory store.",
 * "records":[{"dimensions":[{"name":"dim0","value":"1","dimensionValueType":null}],
 * "measureName":"random_measure_value","measureValue":"1.0","measureValues":null,
 * "measureValueType":"DOUBLE","time":"1797393807000000000",
 * "timeUnit":"NANOSECONDS","version":null}]}]}
 */
@UtilityClass
public class ErrorReportHelper {
    public static List<S3Object> listReportFilesInS3(S3Client s3, String reportBucket, String reportKeyPrefix) {
        ListObjectsResponse response = s3.listObjects(ListObjectsRequest.builder()
                .bucket(reportBucket)
                .prefix(reportKeyPrefix)
                .build());
        return response.contents();
    }

    public static JsonObject readErrorReport(S3Client s3, S3Object object, String reportBucket) {
        return new JsonParser().parse(s3.getObjectAsBytes(GetObjectRequest.builder()
                .key(object.key())
                .bucket(reportBucket)
                .build()).asUtf8String()).getAsJsonObject();
    }
}