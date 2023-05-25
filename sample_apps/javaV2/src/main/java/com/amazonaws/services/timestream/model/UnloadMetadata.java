package com.amazonaws.services.timestream.model;

import java.util.List;

import lombok.Data;
import software.amazon.awssdk.services.timestreamquery.model.ColumnInfo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * POJO for storing metadata information
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true) // To keep it backward compatible for future changes
public class UnloadMetadata {
    @JsonProperty("ColumnInfo")
    List<ColumnInfo> columnInfo;
    @JsonProperty("Author")
    Author author;

    @Data
    public static class Author {
        @JsonProperty("Name")
        String name;
        @JsonProperty("MetadataFileVersion")
        String metadataFileVersion;
    }
}