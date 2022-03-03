package com.amazonaws.samples.kinesis2timestream;

import java.net.URI;
import java.net.URISyntaxException;

import com.amazonaws.ClientConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.ConflictException;
import software.amazon.awssdk.services.timestreamwrite.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.timestreamwrite.model.CreateTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.RetentionProperties;


/**
 * Checks if required database and table exists in Timestream. If they do not exists, it creates them
 */
public class TimestreamInitializer {
    private final TimestreamWriteClient writeClient;

    public TimestreamInitializer(String region, String endpointOverride) {
        if (endpointOverride != null) {
            URI endpointOverrideURI = parseEndpointOverride(endpointOverride);
            this.writeClient = TimestreamWriteClient
                    .builder()
                    .region(Region.of(region))
                    .endpointOverride(endpointOverrideURI)
                    .build();
        } else {
            this.writeClient = TimestreamWriteClient
                    .builder()
                    .region(Region.of(region))
                    .build();
        }
    }

    public void createDatabase(String databaseName) {
        System.out.println("Creating database");
        CreateDatabaseRequest request = CreateDatabaseRequest.builder()
                .databaseName(databaseName)
                .build();
        try {
            writeClient.createDatabase(request);
            System.out.println("Database [" + databaseName + "] created successfully");
        } catch (ConflictException e) {
            System.out.println("Database [" + databaseName + "] exists. Skipping database creation");
        }
    }

    public void createTable(String databaseName, String tableName, long memoryStoreTTLHours, long magneticStoreTTLDays) {
        System.out.println("Creating table");
        final RetentionProperties retentionProperties = RetentionProperties.builder()
                .memoryStoreRetentionPeriodInHours(memoryStoreTTLHours)
                .magneticStoreRetentionPeriodInDays(magneticStoreTTLDays)
                .build();

        CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .databaseName(databaseName)
                .tableName(tableName)
                .retentionProperties(retentionProperties)
                .build();

        try {
            writeClient.createTable(createTableRequest);
            System.out.println("Table [" + tableName + "] successfully created.");
        } catch (ConflictException e) {
            System.out.println("Table [" + tableName + "] exists on database [" + databaseName + "]. Skipping table creation");
        }
    }

    private URI parseEndpointOverride(String endpointOverride) {
        try {
            return new URI(endpointOverride);
        } catch (URISyntaxException uriSyntaxException) {
            throw new RuntimeException("Invalid EndpointOverride Config: " + endpointOverride);
        }
    }
}
