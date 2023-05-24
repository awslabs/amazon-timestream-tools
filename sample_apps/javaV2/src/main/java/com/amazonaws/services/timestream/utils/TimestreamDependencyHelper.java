package com.amazonaws.services.timestream.utils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;

public class TimestreamDependencyHelper {
    private final Region region;
    private final S3Client s3Client;
    private final StsClient stsClient;
    private final S3Utilities s3Utilities;

    public TimestreamDependencyHelper(Region region) {
        this.region = region;
        this.s3Client = S3Client.builder().region(region).build();
        this.stsClient = StsClient.builder().region(region).build();
        this.s3Utilities = s3Client.utilities();
    }

    public String getAccount() {
        return stsClient.getCallerIdentity(GetCallerIdentityRequest.builder().build()).account();
    }

    public String createS3Bucket(final String bucketName) {
        System.out.println("Creating S3 bucket");
        try {
            S3Waiter s3Waiter = s3Client.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3Client.createBucket(bucketRequest);

            // Wait until the bucket is created and print out the response
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return bucketName;
    }

    public void deleteS3Bucket(String bucket) {
        System.out.println("Deleting S3 bucket");
        try {
            // To delete a bucket, all the objects in the bucket must be deleted first
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).build();
            ListObjectsV2Response listObjectsV2Response;

            do {
                listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(s3Object.key())
                            .build());
                }

                listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket)
                        .continuationToken(listObjectsV2Response.nextContinuationToken())
                        .build();

            } while (listObjectsV2Response.isTruncated());
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
            s3Client.deleteBucket(deleteBucketRequest);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public byte[] getObject(String s3URI) throws URISyntaxException {
        // Space needs to encoded to use S3 parseUri function
        S3Uri s3Uri = s3Utilities.parseUri(URI.create(s3URI.replace(" ", "%20")));
        ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(GetObjectRequest.builder()
                .bucket(s3Uri.bucket().orElseThrow(() -> new URISyntaxException(s3URI, "Invalid S3 URI")))
                .key(s3Uri.key().orElseThrow(() -> new URISyntaxException(s3URI, "Invalid S3 URI")))
                .build());
        return objectBytes.asByteArray();
    }

    public void getObjectToLocalFile(String s3URI, String localFilePath) throws IOException, URISyntaxException {
        // Space needs to encoded to use S3 parseUri function
        S3Uri s3Uri = s3Utilities.parseUri(URI.create(s3URI.replace(" ", "%20")));
        GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .bucket(s3Uri.bucket().orElseThrow(() -> new URISyntaxException(s3URI, "Invalid S3 URI")))
                .key(s3Uri.key().orElseThrow(() -> new URISyntaxException(s3URI, "Invalid S3 URI")))
                .build();

        ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
        byte[] data = objectBytes.asByteArray();

        // Write the data to a local file.
        File myFile = new File(localFilePath);
        OutputStream os = Files.newOutputStream(myFile.toPath());
        os.write(data);
        System.out.println("Successfully obtained bytes from an S3 object");
        os.close();
    }
}
