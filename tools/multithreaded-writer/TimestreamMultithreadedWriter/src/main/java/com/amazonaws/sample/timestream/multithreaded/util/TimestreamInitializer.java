package com.amazonaws.sample.timestream.multithreaded.util;

import com.amazonaws.sample.timestream.multithreaded.TimestreamWriterConfig;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.ConflictException;
import software.amazon.awssdk.services.timestreamwrite.model.CreateTableRequest;

public class TimestreamInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(TimestreamInitializer.class);
    private final TimestreamWriterConfig.TimestreamResourceCreationConfig config;
    private final TimestreamWriteClient writeClient;

    public TimestreamInitializer(final TimestreamWriterConfig.TimestreamResourceCreationConfig config,
                                 @NonNull final TimestreamWriteClient writeClient) {

        this.config = config;
        this.writeClient = writeClient;
    }

    public void initialize(@NonNull final String database, @NonNull final String table) {
        if (config != null) {
            try {
                createTable(database, table);
            } catch (final ConflictException e) {
                LOG.info("Table '{}' in database '{}' already exists. " +
                                "Probably it was already created by a concurrent writer.",
                        table, database);
            } catch (final Exception e) {
                LOG.error("Couldn't create table '{}' in database '{}'. Error: ", table, database, e);
            }
        } else {
            LOG.error("Couldn't find table: '{}' in database: '{}'. " +
                            "Automatic table creation was not configured (see 'createTableIfNotExists').",
                    table, database);
        }
    }

    private void createTable(@NonNull String database,
                             @NonNull final String table) {
        LOG.info("Database/Table was not found. Attempting to create table: '{}' in database: '{}'.", table, database);
        final CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .databaseName(database)
                .tableName(table)
                .retentionProperties(config.getRetentionProperties())
                .build();
        writeClient.createTable(createTableRequest);
        LOG.info("Table '{}' in database '{}' successfully created.", table, database);
    }
}
