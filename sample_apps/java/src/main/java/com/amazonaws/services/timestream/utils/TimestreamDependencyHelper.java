package com.amazonaws.services.timestream.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;

import static org.apache.commons.configuration2.convert.PropertyConverter.toFile;

public class TimestreamDependencyHelper {

    private final String region;
    private final AmazonS3 s3Client;
    private final AWSSecurityTokenService sts;

    public TimestreamDependencyHelper(final String region) {
        this.region = region;
        this.s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
        this.sts = AWSSecurityTokenServiceClientBuilder.standard().withRegion(region).build();
    }

    public String getAccount() {
        return sts.getCallerIdentity(new GetCallerIdentityRequest()).getAccount();
    }

    public String createS3Bucket(String bucketName) {
        System.out.println("Creating S3 bucket " + bucketName);
        if (s3Client.doesBucketExistV2(bucketName)) {
            System.out.format("Bucket %s already exists.\n", bucketName);
        } else {
            try {
                s3Client.createBucket(new CreateBucketRequest(bucketName, region));
                System.out.println("Successfully created S3 bucket: " + bucketName);
            } catch (Exception e) {
                System.out.println("Failed to create S3 bucket " + e);
                return null;
            }
        }
        return bucketName;
    }

    public void deleteS3Bucket(String bucketName) {
        System.out.println("Deleting S3 bucket " + bucketName);
        try {
            // Delete all objects from the bucket before deleting the bucket.
            ObjectListing objectListing = s3Client.listObjects(bucketName);
            while (true) {
                for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
                    s3Client.deleteObject(bucketName, s3ObjectSummary.getKey());
                }

                // If the bucket contains many objects, the listObjects() call
                // might not return all of the objects in the first listing. Check to
                // see whether the listing was truncated. If so, retrieve the next page of objects
                // and delete them.
                if (objectListing.isTruncated()) {
                    objectListing = s3Client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }

            // Delete all object versions (required for versioned buckets).
            VersionListing versionList = s3Client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
            while (true) {
                for (S3VersionSummary vs : versionList.getVersionSummaries()) {
                    s3Client.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
                }

                if (versionList.isTruncated()) {
                    versionList = s3Client.listNextBatchOfVersions(versionList);
                } else {
                    break;
                }
            }

            // After all objects and object versions are deleted, delete the bucket.
            s3Client.deleteBucket(bucketName);

        } catch (Exception e) {
            System.out.println("Deletion of S3 bucket failed: " + e.getMessage());
            // Do not throw from here since we need further clean-up actions
        }
    }

    public S3Object getObject(String s3URIString) {
        AmazonS3URI s3URI = new AmazonS3URI(s3URIString);
        return s3Client.getObject(s3URI.getBucket(), s3URI.getKey());
    }

    public void getObjectToLocalFile(String s3URIString, String localFilePath) {
        AmazonS3URI s3URI = new AmazonS3URI(s3URIString);
        final GetObjectRequest request = new GetObjectRequest(s3URI.getBucket(), s3URI.getKey());
        s3Client.getObject(request, toFile(localFilePath));
    }
}
