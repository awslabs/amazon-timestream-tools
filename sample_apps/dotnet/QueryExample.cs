using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.TimestreamQuery;
using Amazon.TimestreamQuery.Model;

namespace TimestreamDotNetSample
{
    public class QueryExample
    {
        private readonly AmazonTimestreamQueryClient queryClient;

        public QueryExample(AmazonTimestreamQueryClient queryClient)
        {
            this.queryClient = queryClient;
        }

        private static string HOSTNAME = "host-24Gju";

        // See records ingested into this table so far
        private static string SELECT_ALL_QUERY = $"SELECT * FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME}";

        //1. Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
        private static string QUERY_1 = $@"SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, 
                                            ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization, 
                                            ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_cpu_utilization, 
                                            ROUND(APPROX_PERCENTILE(measure_value::double, 0.95), 2) AS p95_cpu_utilization, 
                                            ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_cpu_utilization 
                                        FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                        WHERE measure_name = 'cpu_utilization' AND hostname = '{HOSTNAME}' AND time > ago(2h) 
                                        GROUP BY region, hostname, az, BIN(time, 15s) 
                                        ORDER BY binned_timestamp ASC";

        //2. Identify EC2 hosts with CPU utilization that is higher by 10%  or more compared to the average CPU utilization of the entire fleet for the past 2 hours.
        private static string QUERY_2 = $@"WITH avg_fleet_utilization AS (
                                            SELECT COUNT(DISTINCT hostname) AS total_host_count, AVG(measure_value::double) AS fleet_avg_cpu_utilization 
                                            FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                            WHERE measure_name = 'cpu_utilization' 
                                                AND time > ago(2h) 
                                        ), avg_per_host_cpu AS ( 
                                            SELECT region, az, hostname, AVG(measure_value::double) AS avg_cpu_utilization 
                                            FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                            WHERE measure_name = 'cpu_utilization' 
                                                AND time > ago(2h) 
                                            GROUP BY region, az, hostname 
                                        ) 
                                        SELECT region, az, hostname, avg_cpu_utilization, fleet_avg_cpu_utilization 
                                        FROM avg_fleet_utilization, avg_per_host_cpu 
                                        WHERE avg_cpu_utilization > 1.1 * fleet_avg_cpu_utilization 
                                        ORDER BY avg_cpu_utilization DESC";

        //3. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours.
        private static string QUERY_3 = $@"SELECT BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization, 
                                        hostname FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                        WHERE measure_name = 'cpu_utilization' 
                                            AND hostname = '{HOSTNAME}' 
                                            AND time > ago(2h) 
                                        GROUP BY hostname, BIN(time, 30s) 
                                        ORDER BY binned_timestamp ASC";

        //4. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        private static string QUERY_4 = $@"WITH binned_timeseries AS (
                                            SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization 
                                            FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                            WHERE measure_name = 'cpu_utilization' 
                                               AND hostname = '{HOSTNAME}' 
                                                AND time > ago(2h) 
                                            GROUP BY hostname, BIN(time, 30s) 
                                        ), interpolated_timeseries AS ( 
                                            SELECT hostname, 
                                                INTERPOLATE_LINEAR( 
                                                    CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), 
                                                        SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization 
                                            FROM binned_timeseries 
                                            GROUP BY hostname 
                                        ) 
                                        SELECT time, ROUND(value, 2) AS interpolated_cpu 
                                        FROM interpolated_timeseries 
                                        CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)";

        //5. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on the last observation carried forward.
        private static string QUERY_5 = $@"WITH binned_timeseries AS ( 
                                            SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization 
                                            FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                            WHERE measure_name = 'cpu_utilization' 
                                                AND hostname = '{HOSTNAME}' 
                                                AND time > ago(2h) 
                                            GROUP BY hostname, BIN(time, 30s) 
                                        ), interpolated_timeseries AS ( 
                                            SELECT hostname, 
                                                INTERPOLATE_LOCF( 
                                                    CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), 
                                                        SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization 
                                            FROM binned_timeseries 
                                            GROUP BY hostname 
                                        ) 
                                        SELECT time, ROUND(value, 2) AS interpolated_cpu 
                                        FROM interpolated_timeseries 
                                        CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)";

        //6. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on a constant value.
        private static string QUERY_6 = $@"WITH binned_timeseries AS ( 
                                            SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization 
                                            FROM {Constants.DATABASE_NAME }.{Constants.TABLE_NAME} 
                                            WHERE measure_name = 'cpu_utilization' 
                                               AND hostname = '{HOSTNAME}' 
                                                AND time > ago(2h) 
                                            GROUP BY hostname, BIN(time, 30s) 
                                        ), interpolated_timeseries AS ( 
                                            SELECT hostname, 
                                                INTERPOLATE_FILL( 
                                                    CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), 
                                                        SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s), 10.0) AS interpolated_avg_cpu_utilization 
                                            FROM binned_timeseries 
                                            GROUP BY hostname 
                                        ) 
                                        SELECT time, ROUND(value, 2) AS interpolated_cpu 
                                        FROM interpolated_timeseries 
                                        CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)";

        //7. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using cubic spline interpolation.
        private static string QUERY_7 = $@"WITH binned_timeseries AS ( 
                                            SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization 
                                            FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                            WHERE measure_name = 'cpu_utilization' 
                                                AND hostname = '{HOSTNAME}' 
                                                AND time > ago(2h) 
                                            GROUP BY hostname, BIN(time, 30s) 
                                        ), interpolated_timeseries AS ( 
                                            SELECT hostname, 
                                                INTERPOLATE_SPLINE_CUBIC( 
                                                    CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), 
                                                        SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization 
                                            FROM binned_timeseries 
                                            GROUP BY hostname 
                                        ) 
                                        SELECT time, ROUND(value, 2) AS interpolated_cpu 
                                        FROM interpolated_timeseries 
                                        CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)";

        //8. Find the average CPU utilization binned at 30 second intervals for all EC2 hosts over the past 2 hours, filling in the missing values using linear interpolation.
        private static string QUERY_8 = $@"WITH per_host_min_max_timestamp AS ( 
                                            SELECT hostname, min(time) as min_timestamp, max(time) as max_timestamp 
                                            FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                            WHERE measure_name = 'cpu_utilization' 
                                                AND time > ago(2h) 
                                            GROUP BY hostname 
                                        ), interpolated_timeseries AS ( 
                                            SELECT m.hostname, 
                                                INTERPOLATE_LOCF( 
                                                    CREATE_TIME_SERIES(time, measure_value::double), 
                                                        SEQUENCE(MIN(ph.min_timestamp), MAX(ph.max_timestamp), 1s)) as interpolated_avg_cpu_utilization 
                                            FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} m 
                                                INNER JOIN per_host_min_max_timestamp ph ON m.hostname = ph.hostname 
                                            WHERE measure_name = 'cpu_utilization' 
                                                AND time > ago(2h) 
                                            GROUP BY m.hostname 
                                        ) 
                                        SELECT hostname, AVG(cpu_utilization) AS avg_cpu_utilization 
                                        FROM interpolated_timeseries 
                                        CROSS JOIN UNNEST(interpolated_avg_cpu_utilization) AS t (time, cpu_utilization) 
                                        GROUP BY hostname 
                                        ORDER BY avg_cpu_utilization DESC";

        //9. Find the percentage of measurements with CPU utilization above 70% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        private static string QUERY_9 = $@"WITH time_series_view AS ( 
                                            SELECT INTERPOLATE_LINEAR( 
                                                CREATE_TIME_SERIES(time, ROUND(measure_value::double,2)), 
                                                SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization 
                                            FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                            WHERE hostname = '{HOSTNAME}' 
                                                AND  
                                        measure_name = 'cpu_utilization' 
                                                AND time > ago(2h) 
                                            GROUP BY hostname 
                                        ) 
                                        SELECT FILTER(cpu_utilization, x -> x.value > 70.0) AS cpu_above_threshold, 
                                            REDUCE(FILTER(cpu_utilization, x -> x.value > 70.0), 0, (s, x) -> s + 1, s -> s) AS count_cpu_above_threshold, 
                                            ROUND(REDUCE(cpu_utilization, CAST(ROW(0, 0) AS ROW(count_high BIGINT, count_total BIGINT)), 
                                                (s, x) -> CAST(ROW(s.count_high + IF(x.value > 70.0, 1, 0), s.count_total + 1) AS ROW(count_high BIGINT, count_total BIGINT)), 
                                                s -> IF(s.count_total = 0, NULL, CAST(s.count_high AS DOUBLE) / s.count_total)), 4) AS fraction_cpu_above_threshold 
                                        FROM time_series_view";

        //10. List the measurements with CPU utilization lower than 75% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        private static string QUERY_10 = $@"WITH time_series_view AS ( 
                                             SELECT min(time) AS oldest_time, INTERPOLATE_LINEAR( 
                                                 CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), 
                                                 SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization 
                                             FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                             WHERE  
                                          hostname = '{HOSTNAME}' 
                                                 AND  
                                         measure_name = 'cpu_utilization' 
                                                 AND time > ago(2h) 
                                             GROUP BY hostname 
                                         ) 
                                         SELECT FILTER(cpu_utilization, x -> x.value < 75 AND x.time > oldest_time + 1m) 
                                         FROM time_series_view";

        //11. Find the total number of measurements with of CPU utilization of 0% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        private static string QUERY_11 = $@"WITH time_series_view AS ( 
                                             SELECT INTERPOLATE_LINEAR( 
                                                 CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), 
                                                 SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization 
                                             FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                              WHERE  
                                          hostname = '{HOSTNAME}' 
                                                 AND  
                                         measure_name = 'cpu_utilization' 
                                                 AND time > ago(2h) 
                                             GROUP BY hostname 
                                         ) 
                                         SELECT REDUCE(cpu_utilization, 
                                             DOUBLE '0.0', 
                                             (s, x) -> s + 1, 
                                             s -> s) AS count_cpu 
                                         FROM time_series_view";

        //12. Find the average CPU utilization for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        private static string QUERY_12 = $@"WITH time_series_view AS ( 
                                             SELECT INTERPOLATE_LINEAR( 
                                                 CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), 
                                                 SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization 
                                             FROM {Constants.DATABASE_NAME}.{Constants.TABLE_NAME} 
                                              WHERE  
                                          hostname = '{HOSTNAME}' 
                                              AND  
                                         measure_name = 'cpu_utilization' 
                                                 AND time > ago(2h) 
                                             GROUP BY hostname 
                                         ) 
                                         SELECT REDUCE(cpu_utilization, 
                                             CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), 
                                             (s, x) -> CAST(ROW(x.value + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)), 
                                              s -> IF(s.count = 0, NULL, s.sum / s.count)) AS avg_cpu 
                                         FROM time_series_view";

        private static readonly string[] queries =
            {QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6, QUERY_7, QUERY_8, QUERY_9, QUERY_10, QUERY_11, QUERY_12};

        public async Task RunAllQueries()
        {
            for(int i = 0; i < queries.Length; i++)
            {
                Console.WriteLine($"Running query {(i + 1)}:{queries[i]}");
                await RunQueryAsync(queries[i]);    
            }
        }

        private async Task RunQueryAsync(string queryString)
        {
            try
            {
                QueryRequest queryRequest = new QueryRequest();
                queryRequest.QueryString = queryString;
                QueryResponse queryResponse = await queryClient.QueryAsync(queryRequest);
                while (true)
                {
                    ParseQueryResult(queryResponse);
                    if (queryResponse.NextToken == null)
                    {
                        break;
                    }
                    queryRequest.NextToken = queryResponse.NextToken;
                    queryResponse = await queryClient.QueryAsync(queryRequest);
                }
            } catch(Exception e)
            {
                // Some queries might fail with 500 if the result of a sequence function has more than 10000 entries
                Console.WriteLine(e.ToString());
            }
        }

        private void ParseQueryResult(QueryResponse response)
        {
            List<ColumnInfo> columnInfo = response.ColumnInfo;
            var options = new JsonSerializerOptions
            {
                IgnoreNullValues = true
            };
            List<String> columnInfoStrings = columnInfo.ConvertAll(x => JsonSerializer.Serialize(x, options));
            List<Row> rows = response.Rows;

            QueryStatus queryStatus = response.QueryStatus;
            Console.WriteLine("Current Query status:" + JsonSerializer.Serialize(queryStatus, options));
            Console.WriteLine("Metadata:" + string.Join(",", columnInfoStrings));
            Console.WriteLine("Data:");

            foreach (Row row in rows)
            {
                Console.WriteLine(ParseRow(columnInfo, row));
            }
        }

        private string ParseRow(List<ColumnInfo> columnInfo, Row row)
        {
            List<Datum> data = row.Data;
            List<string> rowOutput = new List<string>();
            for (int j = 0; j < data.Count; j++)
            {
                ColumnInfo info = columnInfo[j];
                Datum datum = data[j];
                rowOutput.Add(ParseDatum(info, datum));
            }
            return $"{{{string.Join(",", rowOutput)}}}";
        }

        private string ParseDatum(ColumnInfo info, Datum datum)
        {
            if (datum.NullValue)
            {
                return $"{info.Name}=NULL";
            }

            Amazon.TimestreamQuery.Model.Type columnType = info.Type;
            if (columnType.TimeSeriesMeasureValueColumnInfo != null)
            {
                return ParseTimeSeries(info, datum);
            }
            else if (columnType.ArrayColumnInfo != null)
            {
                List<Datum> arrayValues = datum.ArrayValue;
                return $"{info.Name}={ParseArray(info.Type.ArrayColumnInfo, arrayValues)}";
            }
            else if (columnType.RowColumnInfo != null && columnType.RowColumnInfo.Count > 0)
            {
                List<ColumnInfo> rowColumnInfo = info.Type.RowColumnInfo;
                Row rowValue = datum.RowValue;
                return ParseRow(rowColumnInfo, rowValue);
            }
            else
            {
                return ParseScalarType(info, datum);
            }
        }

        private string ParseTimeSeries(ColumnInfo info, Datum datum)
        {
            var timeseriesString = datum.TimeSeriesValue
                .Select(value => $"{{time={value.Time}, value={ParseDatum(info.Type.TimeSeriesMeasureValueColumnInfo, value.Value)}}}")
                .Aggregate((current, next) => current + "," + next);

            return $"[{timeseriesString}]";
        }

        private string ParseScalarType(ColumnInfo info, Datum datum)
        {
            return ParseColumnName(info) + datum.ScalarValue;
        }

        private string ParseColumnName(ColumnInfo info)
        {
            return info.Name == null ? "" : (info.Name + "=");
        }

        private string ParseArray(ColumnInfo arrayColumnInfo, List<Datum> arrayValues)
        {
            return $"[{arrayValues.Select(value => ParseDatum(arrayColumnInfo, value)).Aggregate((current, next) => current + "," + next)}]";
        }

        public async Task RunQueryWithMultiplePages(int limit)
        {
            string queryWithLimit = $"{SELECT_ALL_QUERY} LIMIT {limit}";
            Console.WriteLine("Starting query with multiple pages: " + queryWithLimit);
            await RunQueryAsync(queryWithLimit);
        }

        public async Task CancelQuery()
        {
            Console.WriteLine("Starting query: " + SELECT_ALL_QUERY);
            QueryRequest queryRequest = new QueryRequest();
            queryRequest.QueryString = SELECT_ALL_QUERY;
            QueryResponse queryResponse = await queryClient.QueryAsync(queryRequest);

            Console.WriteLine("Cancelling query: " + SELECT_ALL_QUERY);
            CancelQueryRequest cancelQueryRequest = new CancelQueryRequest();
            cancelQueryRequest.QueryId = queryResponse.QueryId;

            try
            {
                await queryClient.CancelQueryAsync(cancelQueryRequest);
                Console.WriteLine("Query has been successfully cancelled.");
            } catch(Exception e)
            {
                Console.WriteLine("Could not cancel the query: " + SELECT_ALL_QUERY + " = " + e);
            }
        }
    }
}
