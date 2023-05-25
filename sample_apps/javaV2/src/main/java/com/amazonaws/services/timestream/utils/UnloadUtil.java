package com.amazonaws.services.timestream.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UnknownFormatConversionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import lombok.Getter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.ColumnInfo;
import software.amazon.awssdk.services.timestreamquery.model.QueryResponse;

import com.amazonaws.services.timestream.model.UnloadManifest;
import com.amazonaws.services.timestream.model.UnloadMetadata;
import com.amazonaws.services.timestream.model.UnloadQuery;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import static com.amazonaws.services.timestream.model.UnloadQuery.Format.CSV;
import static com.amazonaws.services.timestream.model.UnloadQuery.Format.PARQUET;
import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.UNLOAD_TABLE_NAME;
import static com.amazonaws.services.timestream.utils.QueryUtil.runQuery;

public class UnloadUtil {
    private static final String QUERY_1 = "SELECT user_id, ip_address, event, session_id, measure_name, time, query, quantity, product_id, channel FROM "
            + DATABASE_NAME + "." + UNLOAD_TABLE_NAME
            + " WHERE time BETWEEN ago(2d) AND now()";
    private static final String QUERY_3 = "SELECT user_id, ip_address, channel, session_id, measure_name, time, query, quantity, product_id, event FROM "
            + DATABASE_NAME + "." + UNLOAD_TABLE_NAME
            + " WHERE time BETWEEN ago(2d) AND now()";

    private static final String QUERY_4 = "SELECT user_id, ip_address, session_id, measure_name, time, query, quantity, product_id, channel,event FROM "
            + DATABASE_NAME + "." + UNLOAD_TABLE_NAME
            + " WHERE time BETWEEN ago(2d) AND now()";

    private final TimestreamQueryClient queryClient;
    private final TimestreamDependencyHelper timestreamDependencyHelper;
    private final List<UnloadQuery> queries;

    public UnloadUtil(final TimestreamQueryClient queryClient,
            final TimestreamDependencyHelper timestreamDependencyHelper,
            final String exportedBucketName) {
        this.queryClient = queryClient;
        this.timestreamDependencyHelper = timestreamDependencyHelper;

        this.queries = new ImmutableList.Builder<UnloadQuery>()
                //Exporting the data without any partitions with CSV format
                .add(UnloadQuery.builder()
                        .selectQuery(QUERY_1)
                        .bucketName(exportedBucketName)
                        .resultsPrefix("without_partition")
                        .format(CSV)
                        .compression(UnloadQuery.Compression.GZIP)
                        .build())
                //Exporting the data without any partitions with PARQUET format
                .add(UnloadQuery.builder()
                        .selectQuery(QUERY_1)
                        .bucketName(exportedBucketName)
                        .resultsPrefix("without_partition")
                        .format(UnloadQuery.Format.PARQUET)
                        .compression(UnloadQuery.Compression.GZIP)
                        .build())
                //Partitioning data by channel
                .add(UnloadQuery.builder()
                        .selectQuery(QUERY_1)
                        .bucketName(exportedBucketName)
                        .resultsPrefix("partition_by_channel")
                        .format(CSV)
                        .compression(UnloadQuery.Compression.GZIP)
                        .partitionColumns(ImmutableList.of("channel"))
                        .build())
                // Partitioning data by event
                .add(UnloadQuery.builder()
                        .selectQuery(QUERY_3)
                        .bucketName(exportedBucketName)
                        .resultsPrefix("partition_by_event")
                        .format(CSV)
                        .compression(UnloadQuery.Compression.GZIP)
                        .partitionColumns(ImmutableList.of("event"))
                        .build())
                // Partitioning data by channel and event
                .add(UnloadQuery.builder()
                        .selectQuery(QUERY_4)
                        .bucketName(exportedBucketName)
                        .resultsPrefix("partition_by_channel_and_event")
                        .format(CSV)
                        .compression(UnloadQuery.Compression.GZIP)
                        .partitionColumns(ImmutableList.of("channel", "event"))
                        .build())
                .build();
    }

    public void runAllQueries() throws IOException, URISyntaxException {
        for (int i = 0; i < queries.size(); i++) {
            String queryString = queries.get(i).getUnloadQuery();
            System.out.println("Running UNLOAD query" + (i + 1) + ": " + queryString);
            List<QueryResponse> queryResponses = runQuery(queryClient, queryString);
            displayResults(queryResponses, queries.get(i));
        }
    }

    public void runSingleQuery() throws IOException, URISyntaxException {
        String queryString = queries.get(0).getUnloadQuery();
        System.out.println("Running UNLOAD query: " + queryString);
        List<QueryResponse> queryResponses = runQuery(queryClient, queryString);
        displayResults(queryResponses, queries.get(0));
    }

    public UnloadManifest getUnloadManifest(UnloadResponse unloadResponse) throws IOException, URISyntaxException {
        String manifestFileContent = new String(timestreamDependencyHelper.getObject(unloadResponse.getManifestFile()),
                StandardCharsets.UTF_8);
        return new Gson().fromJson(manifestFileContent, UnloadManifest.class);
    }

    public UnloadMetadata getUnloadMetadata(UnloadResponse unloadResponse) throws IOException, URISyntaxException {
        String metadataFileContent = new String(timestreamDependencyHelper.getObject(unloadResponse.getMetadataFile()),
                StandardCharsets.UTF_8);
        final Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .create();
        return gson.fromJson(metadataFileContent, UnloadMetadata.class);
    }

    public void displayResults(List<QueryResponse> queryResponses, UnloadQuery unloadQuery) throws IOException, URISyntaxException {
        UnloadResponse unloadResponse = Streams.findLast(queryResponses.stream())
                .map(e -> {
                    Preconditions.checkArgument(e.rows().size() == 1);
                    Map<String, String> outputMap = new HashMap<>();
                    for (int i = 0; i < e.columnInfo().size(); i++) {
                        outputMap.put(e.columnInfo().get(i).name(),
                                e.rows().get(0).data().get(i).scalarValue());

                    }
                    return new UnloadResponse(outputMap);
                }).orElseThrow(() -> new RuntimeException("Could not parse UNLOAD results"));
        System.out.println("Rows exported: " + unloadResponse.getRows());

        UnloadManifest unloadManifest = getUnloadManifest(unloadResponse);
        UnloadMetadata unloadMetadata = getUnloadMetadata(unloadResponse);

        System.out.println("Result File Count: " + unloadManifest.getResult_files().size());
        System.out.println("Columns: \n" + getColumnNames(unloadMetadata, unloadQuery));
        System.out.println("Data: Printing first 10 records:");
        displayAllData(unloadManifest).subList(0, 10).forEach(e -> System.out.println(e.toString()));
    }

    public List<List<String>> displayAllData(UnloadManifest unloadManifest) throws IOException, URISyntaxException {
        List<List<String>> mergedResults = new ArrayList<>();
        for (UnloadManifest.ResultFile resultFile : unloadManifest.getResult_files()) {
            if (CSV.equals(UnloadQuery.Format.valueOf(unloadManifest.getQuery_metadata().getResult_format()))) {
                addCSVRecordsFromFile(mergedResults, timestreamDependencyHelper.getObject(resultFile.getUrl()));
            } else if (PARQUET.equals(UnloadQuery.Format.valueOf(unloadManifest.getQuery_metadata().getResult_format()))) {
                addParquetRecordsFromFile(mergedResults, resultFile);
            } else {
                throw new UnknownFormatConversionException("Format not supported in this sample: " + unloadManifest.getQuery_metadata().getResult_format());
            }
        }
        return mergedResults;
    }

    private void addParquetRecordsFromFile(List<List<String>> mergedResults, UnloadManifest.ResultFile resultFile) throws IOException, URISyntaxException {
        // For Parquet, download it to temporary file to read it with parquet-mr library
        String tempFileName = "/tmp/" + resultFile.getUrl().replace("s3://", "");
        System.out.println("Downloading Parquet file to Local disk");
        timestreamDependencyHelper.getObjectToLocalFile(resultFile.getUrl(), tempFileName);
        try (final ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(tempFileName), new Configuration()))) {
            final MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            PageReadStore pages;
            while (null != (pages = reader.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    final Group g = recordReader.read();
                    int fieldCount = g.getType().getFieldCount();
                    List<String> row = new ArrayList<>();
                    for (int field = 0; field < fieldCount; field++) {
                        int valueCount = g.getFieldRepetitionCount(field);
                        if (valueCount == 0) {
                            row.add(null); // Adding null for empty value
                        } else {
                            Type fieldType = g.getType().getType(field);
                            String fieldName = fieldType.getName();
                            for (int index = 0; index < valueCount; index++) {
                                if (fieldType.isPrimitive()) { // Skipping parsing for complex types
                                    if (fieldName.equals("time")) {
                                        // For time, Timestream exports it as INT96 in Parquet, converting that to epoch in nanos
                                        row.add(String.valueOf(convertINT96ToEpoch(g.getInt96(field, 0).getBytes())));
                                    } else {
                                        row.add(g.getValueToString(field, 0));
                                    }
                                }
                            }
                        }
                    }
                    mergedResults.add(row);
                }
            }
        } finally {
            System.out.println("Deleting Parquet file from Local disk");
            FileUtils.delete(new File(tempFileName));
        }
    }

    private static void addCSVRecordsFromFile(List<List<String>> mergedResults, byte[] fileContent) throws IOException {
        try (CSVParser reader = new CSVParser(new BufferedReader(new InputStreamReader(new GZIPInputStream(
                new ByteArrayInputStream(fileContent)))), CSVFormat.DEFAULT)) {
            reader.stream().forEach(e ->
                    mergedResults.add(StreamSupport
                            .stream(e.spliterator(), false)
                            .collect(Collectors.toList())));
        }
    }

    public List<String> getColumnNames(UnloadMetadata unloadMetadata, UnloadQuery unloadQuery) {
        Set<String> partitionColumns = unloadQuery.getPartitionColumns() == null ? new HashSet<>() :
                new HashSet<>(unloadQuery.getPartitionColumns());
        return unloadMetadata.getColumnInfo().stream()
                .map(ColumnInfo::name)
                // Filter non-partition columns
                .filter(e -> !partitionColumns.contains(e))
                .collect(Collectors.toList());
    }

    @Getter
    static
    class UnloadResponse {
        private final String metadataFile;
        private final String manifestFile;
        private final int rows;

        public UnloadResponse(Map<String, String> unloadResponse) {
            this.metadataFile = unloadResponse.get("metadataFile");
            this.manifestFile = unloadResponse.get("manifestFile");
            this.rows = Integer.parseInt(unloadResponse.get("rows"));
        }
    }

    private static final long NANOS_IN_DAY = TimeUnit.DAYS.toNanos(1);
    // What is Julian Epoch offset? - https://github.com/xhochy/parquet-format/blob/patch-1/LogicalTypes.md#int96-timestamps-also-called-impala_timestamp
    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;

    public static long convertINT96ToEpoch(byte[] bytes) {

        // little endian encoding - need to invert byte order
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return julianDayToNanos(julianDay) + timeOfDayNanos;
    }

    private static long julianDayToNanos(int julianDay) {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * NANOS_IN_DAY;
    }
}
