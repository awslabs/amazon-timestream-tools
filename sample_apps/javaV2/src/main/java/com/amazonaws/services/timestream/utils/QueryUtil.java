package com.amazonaws.services.timestream.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.ColumnInfo;
import software.amazon.awssdk.services.timestreamquery.model.Datum;
import software.amazon.awssdk.services.timestreamquery.model.QueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.QueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.QueryStatus;
import software.amazon.awssdk.services.timestreamquery.model.Row;
import software.amazon.awssdk.services.timestreamquery.model.TimeSeriesDataPoint;
import software.amazon.awssdk.services.timestreamquery.model.Type;
import software.amazon.awssdk.services.timestreamquery.paginators.QueryIterable;

@UtilityClass
public class QueryUtil {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");
    private static final long ONE_GB_IN_BYTES = 1073741824L;

    public static List<QueryResponse> runQuery(TimestreamQueryClient timestreamQueryClient, String queryString) {
        List<QueryResponse> queryResponses = new ArrayList<>();
        try {
            QueryRequest queryRequest = QueryRequest.builder().queryString(queryString).build();
            final QueryIterable queryResponseIterator = timestreamQueryClient.queryPaginator(queryRequest);
            for (QueryResponse queryResponse : queryResponseIterator) {
                parseQueryResult(queryResponse);
                queryResponses.add(queryResponse);
            }
        } catch (Exception e) {
            // Some queries might fail with 500 if the result of a sequence function has more than 10000 entries
            e.printStackTrace();
        }
        return queryResponses;
    }

    private void parseQueryResult(QueryResponse response) {
        final QueryStatus queryStatus = response.queryStatus();

        System.out.println("Query progress so far: " + queryStatus.progressPercentage() + "%");

        double bytesScannedSoFar = (double) queryStatus.cumulativeBytesScanned() / ONE_GB_IN_BYTES;
        System.out.println("Data scanned so far: " + bytesScannedSoFar + " GB");

        double bytesMeteredSoFar = (double) queryStatus.cumulativeBytesMetered() / ONE_GB_IN_BYTES;
        System.out.println("Data metered so far: " + bytesMeteredSoFar + " GB");

        List<ColumnInfo> columnInfo = response.columnInfo();
        List<Row> rows = response.rows();

        System.out.println("Metadata: " + columnInfo);
        System.out.println("Data: ");

        // iterate every row
        for (Row row : rows) {
            System.out.println(parseRow(columnInfo, row));
        }
    }

    private String parseRow(List<ColumnInfo> columnInfo, Row row) {
        List<Datum> data = row.data();
        List<String> rowOutput = new ArrayList<>();
        // iterate every column per row
        for (int j = 0; j < data.size(); j++) {
            ColumnInfo info = columnInfo.get(j);
            Datum datum = data.get(j);
            rowOutput.add(parseDatum(info, datum));
        }
        return String.format("{%s}", rowOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    private String parseDatum(ColumnInfo info, Datum datum) {
        if (datum.nullValue() != null && datum.nullValue()) {
            return info.name() + "=" + "NULL";
        }
        Type columnType = info.type();
        // If the column is of TimeSeries Type
        if (columnType.timeSeriesMeasureValueColumnInfo() != null) {
            return parseTimeSeries(info, datum);
        }
        // If the column is of Array Type
        else if (columnType.arrayColumnInfo() != null) {
            List<Datum> arrayValues = datum.arrayValue();
            return info.name() + "=" + parseArray(info.type().arrayColumnInfo(), arrayValues);
        }
        // If the column is of Row Type
        else if (columnType.rowColumnInfo() != null && columnType.rowColumnInfo().size() > 0) {
            List<ColumnInfo> rowColumnInfo = info.type().rowColumnInfo();
            Row rowValues = datum.rowValue();
            return parseRow(rowColumnInfo, rowValues);
        }
        // If the column is of Scalar Type
        else {
            return parseScalarType(info, datum);
        }
    }

    private String parseTimeSeries(ColumnInfo info, Datum datum) {
        List<String> timeSeriesOutput = new ArrayList<>();
        for (TimeSeriesDataPoint dataPoint : datum.timeSeriesValue()) {
            timeSeriesOutput.add("{time=" + dataPoint.time() + ", value=" +
                    parseDatum(info.type().timeSeriesMeasureValueColumnInfo(), dataPoint.value()) + "}");
        }
        return String.format("[%s]", timeSeriesOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    private String parseScalarType(ColumnInfo info, Datum datum) {
        switch (info.type().scalarType()) {
            case VARCHAR:
                return parseColumnName(info) + datum.scalarValue();
            case BIGINT:
                Long longValue = Long.valueOf(datum.scalarValue());
                return parseColumnName(info) + longValue;
            case INTEGER:
                Integer intValue = Integer.valueOf(datum.scalarValue());
                return parseColumnName(info) + intValue;
            case BOOLEAN:
                Boolean booleanValue = Boolean.valueOf(datum.scalarValue());
                return parseColumnName(info) + booleanValue;
            case DOUBLE:
                Double doubleValue = Double.valueOf(datum.scalarValue());
                return parseColumnName(info) + doubleValue;
            case TIMESTAMP:
                return parseColumnName(info) + LocalDateTime.parse(datum.scalarValue(), TIMESTAMP_FORMATTER);
            case DATE:
                return parseColumnName(info) + LocalDate.parse(datum.scalarValue(), DATE_FORMATTER);
            case TIME:
                return parseColumnName(info) + LocalTime.parse(datum.scalarValue(), TIME_FORMATTER);
            case INTERVAL_DAY_TO_SECOND:
            case INTERVAL_YEAR_TO_MONTH:
                return parseColumnName(info) + datum.scalarValue();
            case UNKNOWN:
                return parseColumnName(info) + datum.scalarValue();
            default:
                throw new IllegalArgumentException("Given type is not valid: " + info.type().scalarType());
        }
    }

    private String parseColumnName(ColumnInfo info) {
        return info.name() == null ? "" : info.name() + "=";
    }

    private String parseArray(ColumnInfo arrayColumnInfo, List<Datum> arrayValues) {
        List<String> arrayOutput = new ArrayList<>();
        for (Datum datum : arrayValues) {
            arrayOutput.add(parseDatum(arrayColumnInfo, datum));
        }
        return String.format("[%s]", arrayOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}
