#!/usr/bin/python
import sys
import traceback


class QueryUtil:
    HOSTNAME = "host-24Gju"

    def __init__(self, client, database_name, table_name):
        self.client = client
        self.paginator = client.get_paginator('query')

        # See records ingested into this table so far
        self.SELECT_ALL = "SELECT * FROM " + database_name + "." + table_name

        # 1. Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
        QUERY_1 = \
            "SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, " \
            "    ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization, " \
            "    ROUND(APPROX_PERCENTILE(cpu_utilization, 0.9), 2) AS p90_cpu_utilization, " \
            "    ROUND(APPROX_PERCENTILE(cpu_utilization, 0.95), 2) AS p95_cpu_utilization, " \
            "    ROUND(APPROX_PERCENTILE(cpu_utilization, 0.99), 2) AS p99_cpu_utilization " \
            "FROM " + database_name + "." + table_name + " " \
            "WHERE measure_name = 'metrics' " \
            "   AND hostname = '" + self.HOSTNAME + "' " \
            "    AND time > ago(2h) " \
            "GROUP BY region, hostname, az, BIN(time, 15s) " \
            "ORDER BY binned_timestamp ASC"

        # 2. Identify EC2 hosts with CPU utilization that is higher by 10%  or more compared to the average CPU utilization of the entire fleet for the past 2 hours.
        QUERY_2 = \
            "WITH avg_fleet_utilization AS ( " \
            "    SELECT COUNT(DISTINCT hostname) AS total_host_count, AVG(cpu_utilization) AS fleet_avg_cpu_utilization " \
                    "    FROM " + database_name + "." + table_name + " " \
            "    WHERE measure_name = 'metrics' " \
            "        AND time > ago(2h) " \
            "), avg_per_host_cpu AS ( " \
            "    SELECT region, az, hostname, AVG(cpu_utilization) AS avg_cpu_utilization " \
            "    FROM " +  database_name + "." +  table_name + " " \
            "    WHERE measure_name = 'metrics' " \
            "        AND time > ago(2h) " \
            "    GROUP BY region, az, hostname " \
            ") " \
            "SELECT region, az, hostname, avg_cpu_utilization, fleet_avg_cpu_utilization " \
            "FROM avg_fleet_utilization, avg_per_host_cpu " \
            "WHERE avg_cpu_utilization > 1.1 * fleet_avg_cpu_utilization " \
            "ORDER BY avg_cpu_utilization DESC"

        # 3. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours.
        QUERY_3 = \
            "SELECT BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization, " \
            "hostname FROM " +  database_name + "." +  table_name + " " \
            "WHERE measure_name = 'metrics' " \
            "    AND hostname = '" + self.HOSTNAME + "' " \
            "    AND time > ago(2h) " \
            "GROUP BY hostname, BIN(time, 30s) " \
            "ORDER BY binned_timestamp ASC"

        # 4. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        QUERY_4 = \
            "WITH binned_timeseries AS ( " \
            "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization " \
            "    FROM " +  database_name + "." +  table_name + " " \
            "    WHERE measure_name = 'metrics' " \
            "       AND hostname = '" + self.HOSTNAME + "' " \
            "        AND time > ago(2h) " \
            "    GROUP BY hostname, BIN(time, 30s) " \
            "), interpolated_timeseries AS ( " \
            "    SELECT hostname, " \
            "        INTERPOLATE_LINEAR( " \
            "            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " \
            "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization " \
            "    FROM binned_timeseries " \
            "    GROUP BY hostname " \
            ") " \
            "SELECT time, ROUND(value, 2) AS interpolated_cpu " \
            "FROM interpolated_timeseries " \
            "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)"

        # 5. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on the last observation carried forward.
        QUERY_5 = \
            "WITH binned_timeseries AS ( " \
            "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization " \
            "    FROM " +  database_name + "." +  table_name + " " \
            "    WHERE measure_name = 'metrics' " \
            "        AND hostname = '" + self.HOSTNAME + "' " \
            "        AND time > ago(2h) " \
            "    GROUP BY hostname, BIN(time, 30s) " \
            "), interpolated_timeseries AS ( " \
            "    SELECT hostname, " \
            "        INTERPOLATE_LOCF( " \
            "            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " \
            "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization " \
            "    FROM binned_timeseries " \
            "    GROUP BY hostname " \
            ") " \
            "SELECT time, ROUND(value, 2) AS interpolated_cpu " \
            "FROM interpolated_timeseries " \
            "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)"

        # 6. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on a constant value.
        QUERY_6 = \
            "WITH binned_timeseries AS ( " \
            "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization " \
            "    FROM " +  database_name + "." +  table_name + " " \
            "    WHERE measure_name = 'metrics' " \
            "       AND hostname = '" + self.HOSTNAME + "' " \
            "        AND time > ago(2h) " \
            "    GROUP BY hostname, BIN(time, 30s) " \
            "), interpolated_timeseries AS ( " \
            "    SELECT hostname, " \
            "        INTERPOLATE_FILL( " \
            "            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " \
            "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s), 10.0) AS interpolated_avg_cpu_utilization " \
            "    FROM binned_timeseries " \
            "    GROUP BY hostname " \
            ") " \
            "SELECT time, ROUND(value, 2) AS interpolated_cpu " \
            "FROM interpolated_timeseries " \
            "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)"

        # 7. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using cubic spline interpolation.
        QUERY_7 = \
            "WITH binned_timeseries AS ( " \
            "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization " \
            "    FROM " +  database_name + "." +  table_name + " " \
            "    WHERE measure_name = 'metrics' " \
            "        AND hostname = '" + self.HOSTNAME + "' " \
            "        AND time > ago(2h) " \
            "    GROUP BY hostname, BIN(time, 30s) " \
            "), interpolated_timeseries AS ( " \
            "    SELECT hostname, " \
            "        INTERPOLATE_SPLINE_CUBIC( " \
            "            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " \
            "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization " \
            "    FROM binned_timeseries " \
            "    GROUP BY hostname " \
            ") " \
            "SELECT time, ROUND(value, 2) AS interpolated_cpu " \
            "FROM interpolated_timeseries " \
            "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)"

        # 8. Find the average CPU utilization binned at 30 second intervals for all EC2 hosts over the past 2 hours, filling in the missing values using LOCF interpolation.
        QUERY_8 = \
            "WITH per_host_min_max_timestamp AS ( " \
            "    SELECT hostname, min(time) as min_timestamp, max(time) as max_timestamp " \
            "    FROM " +  database_name + "." +  table_name + " " \
            "    WHERE measure_name = 'metrics' " \
            "        AND time > ago(2h) " \
            "    GROUP BY hostname " \
            "), interpolated_timeseries AS ( " \
            "    SELECT m.hostname, " \
            "        INTERPOLATE_LOCF( " \
            "            CREATE_TIME_SERIES(time, cpu_utilization), " \
            "                SEQUENCE(MIN(ph.min_timestamp), MAX(ph.max_timestamp), 1s)) as interpolated_avg_cpu_utilization " \
            "    FROM " +  database_name + "." +  table_name + " m " \
            "        INNER JOIN per_host_min_max_timestamp ph ON m.hostname = ph.hostname " \
            "    WHERE measure_name = 'metrics' " \
            "        AND time > ago(2h) " \
            "    GROUP BY m.hostname " \
            ") " \
            "SELECT hostname, AVG(cpu_utilization) AS avg_cpu_utilization " \
            "FROM interpolated_timeseries " \
            "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization) AS t (time, cpu_utilization) " \
            "GROUP BY hostname " \
            "ORDER BY avg_cpu_utilization DESC"

        # 9. Find the percentage of measurements with CPU utilization above 70% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        QUERY_9 = \
            "WITH time_series_view AS ( " \
            "    SELECT INTERPOLATE_LINEAR( " \
            "        CREATE_TIME_SERIES(time, ROUND(cpu_utilization, 2)), " \
            "        SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization " \
            "    FROM " +  database_name + "." +  table_name + " " \
            "    WHERE hostname = '" + self.HOSTNAME + "' " \
            "        AND  " \
            "measure_name = 'metrics' " \
            "        AND time > ago(2h) " \
            "    GROUP BY hostname " \
            ") " \
            "SELECT FILTER(interpolated_cpu_utilization, x -> x.value > 70.0) AS cpu_above_threshold, " \
            "    REDUCE(FILTER(interpolated_cpu_utilization, x -> x.value > 70.0), 0, (s, x) -> s + 1, s -> s) AS count_cpu_above_threshold, " \
            "    ROUND(REDUCE(interpolated_cpu_utilization, CAST(ROW(0, 0) AS ROW(count_high BIGINT, count_total BIGINT)), " \
            "        (s, x) -> CAST(ROW(s.count_high + IF(x.value > 70.0, 1, 0), s.count_total + 1) AS ROW(count_high BIGINT, count_total BIGINT)), " \
            "        s -> IF(s.count_total = 0, NULL, CAST(s.count_high AS DOUBLE) / s.count_total)), 4) AS fraction_cpu_above_threshold " \
            "FROM time_series_view"

        # 10. List the measurements with CPU utilization lower than 75% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        QUERY_10 = \
            "WITH time_series_view AS ( " \
            "    SELECT min(time) AS oldest_time, INTERPOLATE_LINEAR( " \
            "        CREATE_TIME_SERIES(time, ROUND(cpu_utilization, 2)), " \
            "        SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization " \
            "    FROM " +  database_name + "." +  table_name + " " \
            "    WHERE  " \
            " hostname = '" + self.HOSTNAME + "' " \
            "        AND  " \
            "measure_name = 'metrics' " \
            "        AND time > ago(2h) " \
            "    GROUP BY hostname " \
            ") " \
            "SELECT FILTER(interpolated_cpu_utilization, x -> x.value < 75 AND x.time > oldest_time + 1m) " \
            "FROM time_series_view"

        # 11. Find the total number of measurements with of CPU utilization of 0% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        QUERY_11 = \
            "WITH time_series_view AS ( " \
            "    SELECT INTERPOLATE_LINEAR( " \
            "        CREATE_TIME_SERIES(time, ROUND(cpu_utilization, 2)), " \
            "        SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization " \
            "    FROM " +  database_name + "." +  table_name + " " \
            "     WHERE  " \
            " hostname = '" + self.HOSTNAME + "' " \
            "        AND  " \
            "measure_name = 'metrics' " \
            "        AND time > ago(2h) " \
            "    GROUP BY hostname " \
            ") " \
            "SELECT REDUCE(interpolated_cpu_utilization, " \
            "    DOUBLE '0.0', " \
            "    (s, x) -> s + 1, " \
            "    s -> s) AS count_cpu " \
            "FROM time_series_view"

        # 12. Find the average CPU utilization for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        QUERY_12 = \
            "WITH time_series_view AS ( " \
            "    SELECT INTERPOLATE_LINEAR( " \
            "        CREATE_TIME_SERIES(time, ROUND(cpu_utilization, 2)), " \
            "        SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization " \
            "    FROM " +  database_name + "." +  table_name + " " \
            "     WHERE  " \
            " hostname = '" + self.HOSTNAME + "' " \
            "     AND  " \
            "measure_name = 'metrics' " \
            "        AND time > ago(2h) " \
            "    GROUP BY hostname " \
            ") " \
            "SELECT REDUCE(interpolated_cpu_utilization, " \
            "    CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), " \
            "    (s, x) -> CAST(ROW(x.value + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)), " \
            "     s -> IF(s.count = 0, NULL, s.sum / s.count)) AS avg_cpu " \
            "FROM time_series_view"


        self.queries = [QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6, QUERY_7, QUERY_8, QUERY_9, QUERY_10, QUERY_11, QUERY_12]

    def run_all_queries(self):
        for query_id in range(len(self.queries)):
            print("Running query [%d] : [%s]" % (query_id + 1, self.queries[query_id]))
            self.run_query(self.queries[query_id])

    def run_simple_select_all_query(self):
        self.run_query(QUERY_SELECT_ALL)

    def run_query(self, query_string):
        try:
            page_iterator = self.paginator.paginate(QueryString=query_string)
            for page in page_iterator:
                self.__parse_query_result(page)
        except Exception as err:
            print("Exception while running query:", err)
            traceback.print_exc(file=sys.stderr)

    def __parse_query_result(self, query_result):
        column_info = query_result['ColumnInfo']

        print("Metadata: %s" % column_info)
        print("Data: ")
        for row in query_result['Rows']:
            print(self.__parse_row(column_info, row))

    def __parse_row(self, column_info, row):
        data = row['Data']
        row_output = []
        for j in range(len(data)):
            info = column_info[j]
            datum = data[j]
            row_output.append(self.__parse_datum(info, datum))

        return "{%s}" % str(row_output)

    def __parse_datum(self, info, datum):
        if datum.get('NullValue', False):
            return "%s=NULL" % info['Name']

        column_type = info['Type']

        # If the column is of TimeSeries Type
        if 'TimeSeriesMeasureValueColumnInfo' in column_type:
            return self.__parse_time_series(info, datum)

        # If the column is of Array Type
        elif 'ArrayColumnInfo' in column_type:
            array_values = datum['ArrayValue']
            return "%s=%s" % (info['Name'], self.__parse_array(info['Type']['ArrayColumnInfo'], array_values))

        # If the column is of Row Type
        elif 'RowColumnInfo' in column_type:
            row_column_info = info['Type']['RowColumnInfo']
            row_values = datum['RowValue']
            return self.__parse_row(row_column_info, row_values)

        # If the column is of Scalar Type
        else:
            return self.__parse_column_name(info) + datum['ScalarValue']

    def __parse_time_series(self, info, datum):
        time_series_output = [
            f"{{time={data_point['Time']}, value={self.__parse_datum(info['Type']['TimeSeriesMeasureValueColumnInfo'],data_point['Value'])}}}"
            for data_point in datum['TimeSeriesValue']
        ]

        return f"[{time_series_output}]"

    def __parse_column_name(self, info):
        if 'Name' in info:
            return info['Name'] + "="
        else:
            return ""

    def __parse_array(self, array_column_info, array_values):
        array_output = [
            f"{self.__parse_datum(array_column_info, datum)}"
            for datum in array_values
        ]

        return f"[{array_output}]"

    def run_query_with_multiple_pages(self, limit=None):
        query_with_limit = self.SELECT_ALL
        if limit is not None:
            query_with_limit += " LIMIT " + str(limit)
        print("Starting query with multiple pages : " + query_with_limit)
        self.run_query(query_with_limit)

    def cancel_query(self):
        print("Starting query: " + self.SELECT_ALL)
        result = self.client.query(QueryString=self.SELECT_ALL)
        print("Cancelling query: " + self.SELECT_ALL)
        try:
            self.client.cancel_query(QueryId=result['QueryId'])
            print("Query has been successfully cancelled")
        except Exception as err:
            print("Cancelling query failed:", err)

