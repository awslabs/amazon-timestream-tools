package com.amazonaws.services.timestream.model;

import java.util.List;

import lombok.Data;
import lombok.Getter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


/**
 * POJO for storing manifest information
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true) // To keep it backward compatible for future changes
public class UnloadManifest {
    @Getter
    public static class FileMetadata {
        long content_length_in_bytes;
        long row_count;
    }

    @Getter
    public static class ResultFile {
        String url;
        FileMetadata file_metadata;
    }

    @Getter
    public static class QueryMetadata {
        long total_content_length_in_bytes;
        long total_row_count;
        String result_format;
        String result_version;
    }

    @Getter
    public static class Author {
        String name;
        String manifest_file_version;
    }

    @Getter
    private List<ResultFile> result_files;
    @Getter
    private QueryMetadata query_metadata;
    @Getter
    private Author author;
}