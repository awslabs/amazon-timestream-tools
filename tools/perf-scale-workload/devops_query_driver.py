##################################################
## A multi-process and multi-threaded driver #####
## that executes the specified query workload ####
## simulating concurrent user sessions querying ##
## recent and historical data ingested into ######
## the specified database and table in Timestream
##################################################

import query_executer as tsb_query
from query_execution_utils import Query
import timestreamquery as tsquery
import argparse
import os
from pathlib import Path
import multiprocessing
from timeit import default_timer as timer
import datetime

######################################
########## Query Workload  ###########
######################################

##
## This query emulates an alerting use-case where the goal is to obtain the aggregate values
## of a metric for the specified time period for a given region, cell, silo, availability_zone
## and microservice_name. The application can fire an alert if the metric aggregate values are
## above or below a threshold.
##
q1Str = """
SELECT region, cell, silo, availability_zone, microservice_name,
    BIN(time, 1m) AS time_bin,
    COUNT(DISTINCT instance_name) AS num_hosts,
    ROUND(AVG(measure_value::double), 2) AS avg_value,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_value,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.95), 2) AS p95_value,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_value
FROM {0}.{1}
WHERE time BETWEEN {2} AND {3}
    AND measure_name = '{4}'
    AND region = '{5}' AND cell = '{6}'
    AND silo = '{7}' AND availability_zone = '{8}'
    AND microservice_name = '{9}'
GROUP BY region, cell, silo, availability_zone, microservice_name, BIN(time, 1m)
ORDER BY p99_value DESC
"""

##
## Variant of Q1 when data is emitted in the MULTI model.
##
q1StrMulti = """
SELECT region, cell, silo, availability_zone, microservice_name,
    BIN(time, 1m) AS time_bin,
    COUNT(DISTINCT instance_name) AS num_hosts,
    ROUND(AVG({4}), 2) AS avg_value,
    ROUND(APPROX_PERCENTILE({4}, 0.9), 2) AS p90_value,
    ROUND(APPROX_PERCENTILE({4}, 0.95), 2) AS p95_value,
    ROUND(APPROX_PERCENTILE({4}, 0.99), 2) AS p99_value
FROM {0}.{1}
WHERE time BETWEEN {2} AND {3}
    AND measure_name = 'metrics'
    AND region = '{5}' AND cell = '{6}'
    AND silo = '{7}' AND availability_zone = '{8}'
    AND microservice_name = '{9}'
GROUP BY region, cell, silo, availability_zone, microservice_name, BIN(time, 1m)
ORDER BY p99_value DESC
"""

##
## This query emulates an alerting use-case where the goal is to obtain the aggregate values
## of multiple metrics, specifically cpu_user and cpu_system, for the specified time period
## for a given region, cell, silo, availability_zone, microservice_name, instance_type, and
## os_version. The application can fire an alert if the metric aggregate values are
## above or below a threshold.
##
q2Str = """
SELECT BIN(time, 1m) AS time_bin,
    AVG(CASE WHEN measure_name = 'cpu_user' THEN measure_value::double ELSE NULL END) AS avg_cpu_user,
    AVG(CASE WHEN measure_name = 'cpu_system' THEN measure_value::double ELSE NULL END) AS avg_cpu_system
FROM {0}.{1}
WHERE time BETWEEN {2} AND {3}
    AND measure_name IN (
        'cpu_user', 'cpu_system'
    )
    AND region = '{4}' AND cell = '{5}' AND silo = '{6}'
    AND availability_zone = '{7}' AND microservice_name = '{8}'
    AND instance_type = '{9}' AND os_version = '{10}'
GROUP BY BIN(time, 1m)
ORDER BY time_bin desc
"""

##
## Variant of Q2 when data is emitted in the MULTI model
##
q2StrMulti = """
SELECT BIN(time, 1m) AS time_bin,
    AVG(cpu_user) AS avg_cpu_user,
    AVG(cpu_system) AS avg_cpu_system
FROM {0}.{1}
WHERE time BETWEEN {2} AND {3}
    AND measure_name = 'metrics'
    AND region = '{4}' AND cell = '{5}' AND silo = '{6}'
    AND availability_zone = '{7}' AND microservice_name = '{8}'
    AND instance_type = '{9}' AND os_version = '{10}'
GROUP BY BIN(time, 1m)
ORDER BY time_bin desc
"""

##
## This query emulates populating a dashboard identifying the instances in a given region,
## and microservice name whose aggregate read of a specifed measure_name is above the
## region-wide aggregate of the metric.
##
q3Str = """
WITH microservice_cell_avg AS (
    SELECT cell, microservice_name,
        AVG(measure_value::double) AS microservice_avg_metric
    FROM {0}.{1}
    WHERE time BETWEEN {2} AND {3}
        AND region = '{4}'
        AND measure_name = '{5}'
        AND microservice_name = '{6}'
    GROUP BY cell, microservice_name
), instance_avg AS (
    SELECT cell, microservice_name, instance_name,
        AVG(measure_value::double) AS instance_avg_metric
    FROM {0}.{1}
    WHERE time BETWEEN {2} AND {3}
        AND region = '{4}'
        AND measure_name = '{5}'
        AND microservice_name = '{6}'
    GROUP BY instance_name, cell, microservice_name
)
SELECT i.*, m.microservice_avg_metric
FROM microservice_cell_avg m INNER JOIN instance_avg i
    ON i.cell = m.cell AND i.microservice_name = m.microservice_name
WHERE i.instance_avg_metric > (1 + {7}) * m.microservice_avg_metric
ORDER BY i.instance_avg_metric DESC
"""

##
## Variant of Q3 when data is emitted in the MULTI model
##
q3StrMulti = """
WITH microservice_cell_avg AS (
    SELECT cell, microservice_name,
        AVG({5}) AS microservice_avg_metric
    FROM {0}.{1}
    WHERE time BETWEEN {2} AND {3}
        AND region = '{4}'
        AND measure_name = 'metrics'
        AND microservice_name = '{6}'
    GROUP BY cell, microservice_name
), instance_avg AS (
    SELECT cell, microservice_name, instance_name,
        AVG({5}) AS instance_avg_metric
    FROM {0}.{1}
    WHERE time BETWEEN {2} AND {3}
        AND region = '{4}'
        AND measure_name = 'metrics'
        AND microservice_name = '{6}'
    GROUP BY instance_name, cell, microservice_name
)
SELECT i.*, m.microservice_avg_metric
FROM microservice_cell_avg m INNER JOIN instance_avg i
    ON i.cell = m.cell AND i.microservice_name = m.microservice_name
WHERE i.instance_avg_metric > (1 + {7}) * m.microservice_avg_metric
ORDER BY i.instance_avg_metric DESC
"""

##
## This query emulates populating a dashboard with the hourly distribution of different resource
## utilization metrics of the specified microservice_name across differen regions, cells, and
## silos for the past several hours.
##
q4Str = """
SELECT region, silo, cell, microservice_name, BIN(time, 1h) AS hour,
    COUNT(CASE WHEN measure_name = 'cpu_user' THEN measure_value::double ELSE NULL END) AS num_cpu_user_samples,
    ROUND(AVG(CASE WHEN measure_name = 'cpu_user' THEN measure_value::double ELSE NULL END), 2) AS avg_cpu_user,
    ROUND(MAX(CASE WHEN measure_name = 'cpu_user' THEN measure_value::double ELSE NULL END), 2) AS max_cpu_user,
    COUNT(CASE WHEN measure_name = 'memory_used' THEN measure_value::double ELSE NULL END) AS num_memory_used_samples,
    ROUND(AVG(CASE WHEN measure_name = 'memory_used' THEN measure_value::double ELSE NULL END), 2) AS avg_memory_used,
    ROUND(MAX(CASE WHEN measure_name = 'memory_used' THEN measure_value::double ELSE NULL END), 2) AS max_memory_used
FROM {0}.{1}
WHERE time BETWEEN {2} AND {3}
    AND measure_name IN (
        'cpu_user', 'memory_used'
    )
    AND microservice_name = '{4}'
GROUP BY silo, cell, region, microservice_name, BIN(time, 1h)
ORDER BY region, silo, cell, microservice_name, hour DESC
"""

##
## Variant of Q4 when data is emitted in the MULTI model
##
q4StrMulti = """
SELECT region, silo, cell, microservice_name, BIN(time, 1h) AS hour,
    COUNT(cpu_user) AS num_cpu_user_samples,
    ROUND(AVG(cpu_user), 2) AS avg_cpu_user,
    ROUND(MAX(cpu_user), 2) AS max_cpu_user,
    COUNT(memory_used) AS num_memory_used_samples,
    ROUND(AVG(memory_used), 2) AS avg_memory_used,
    ROUND(MAX(memory_used), 2) AS max_memory_used
FROM {0}.{1}
WHERE time BETWEEN {2} AND {3}
    AND measure_name = 'metrics'
    AND microservice_name = '{4}'
GROUP BY silo, cell, region, microservice_name, BIN(time, 1h)
ORDER BY region, silo, cell, microservice_name, hour DESC
"""

##
## This query emulates an analysis scenario to find the instances with the highest memory
## utilization across the different microservices and then compute the distribution of gc_pause for those
## instances over the past day. The region, cell, silo, and availability_zone are specified.
##
q5Str = """
WITH per_instance_memory_used AS (
    SELECT silo, microservice_name, instance_name, BIN(time, 5m) AS time_bin,
        MAX(measure_value::double) AS max_memory
    FROM {0}.{1}
    WHERE time BETWEEN {2} AND {3}
        AND measure_name = 'memory_used'
        AND region = '{4}' AND cell = '{5}'
        AND silo = '{6}' AND availability_zone = '{7}'
    GROUP BY microservice_name, instance_name, BIN(time, 5m), silo
), per_microservice_memory AS (
    SELECT silo, microservice_name,
        APPROX_PERCENTILE(max_memory, 0.95) AS p95_max_memory
    FROM per_instance_memory_used
    GROUP BY silo, microservice_name
), per_silo_ranked AS (
    SELECT silo, microservice_name,
        DENSE_RANK() OVER (PARTITION BY silo ORDER BY p95_max_memory DESC) AS rank
    FROM per_microservice_memory
), instances_with_high_memory AS (
    SELECT r.silo, r.microservice_name, m.instance_name,
        APPROX_PERCENTILE(max_memory, 0.95) AS p95_max_memory
    FROM per_silo_ranked r INNER JOIN per_instance_memory_used m
        ON r.silo = m.silo AND r.microservice_name = m.microservice_name
    WHERE r.rank = 1
    GROUP BY m.instance_name, r.silo, r.microservice_name
), ranked_instances AS (
    SELECT silo, microservice_name, instance_name,
        DENSE_RANK() OVER (PARTITION BY silo, microservice_name ORDER BY p95_max_memory DESC) AS rank
    FROM instances_with_high_memory
)
SELECT t.silo, t.microservice_name, t.instance_name,
    MIN(measure_value::double) AS min_gc_pause,
    ROUND(AVG(measure_value::double), 2) AS avg_gc_pause,
    ROUND(STDDEV(measure_value::double), 2) AS stddev_gc_pause,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.5), 2) AS p50_gc_pause,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_gc_pause,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_gc_pause
FROM ranked_instances r INNER JOIN {0}.{1} t ON
    r.silo = t.silo AND
    r.microservice_name = t.microservice_name AND r.instance_name = t.instance_name
WHERE time BETWEEN {2} AND {3}
    AND measure_name = 'gc_pause' AND rank = 1
GROUP BY t.instance_name, t.silo, t.microservice_name
"""

##
## Variant of Q5 when data is emitted in the MULTI model
##
q5StrMulti = """
WITH per_instance_memory_used AS (
    SELECT silo, microservice_name, instance_name, BIN(time, 5m) AS time_bin,
        MAX(memory_used) AS max_memory
    FROM {0}.{1}
    WHERE time BETWEEN {2} AND {3}
        AND measure_name = 'metrics'
        AND region = '{4}' AND cell = '{5}'
        AND silo = '{6}' AND availability_zone = '{7}'
    GROUP BY microservice_name, instance_name, BIN(time, 5m), silo
), per_microservice_memory AS (
    SELECT silo, microservice_name,
        APPROX_PERCENTILE(max_memory, 0.95) AS p95_max_memory
    FROM per_instance_memory_used
    GROUP BY silo, microservice_name
), per_silo_ranked AS (
    SELECT silo, microservice_name,
        DENSE_RANK() OVER (PARTITION BY silo ORDER BY p95_max_memory DESC) AS rank
    FROM per_microservice_memory
), instances_with_high_memory AS (
    SELECT r.silo, r.microservice_name, m.instance_name,
        APPROX_PERCENTILE(max_memory, 0.95) AS p95_max_memory
    FROM per_silo_ranked r INNER JOIN per_instance_memory_used m
        ON r.silo = m.silo AND r.microservice_name = m.microservice_name
    WHERE r.rank = 1
    GROUP BY m.instance_name, r.silo, r.microservice_name
), ranked_instances AS (
    SELECT silo, microservice_name, instance_name,
        DENSE_RANK() OVER (PARTITION BY silo, microservice_name ORDER BY p95_max_memory DESC) AS rank
    FROM instances_with_high_memory
)
SELECT t.silo, t.microservice_name, t.instance_name,
    MIN(gc_pause) AS min_gc_pause,
    ROUND(AVG(gc_pause), 2) AS avg_gc_pause,
    ROUND(STDDEV(gc_pause), 2) AS stddev_gc_pause,
    ROUND(APPROX_PERCENTILE(gc_pause, 0.5), 2) AS p50_gc_pause,
    ROUND(APPROX_PERCENTILE(gc_pause, 0.9), 2) AS p90_gc_pause,
    ROUND(APPROX_PERCENTILE(gc_pause, 0.99), 2) AS p99_gc_pause
FROM ranked_instances r INNER JOIN {0}.{1} t ON
    r.silo = t.silo AND
    r.microservice_name = t.microservice_name AND r.instance_name = t.instance_name
WHERE time BETWEEN {2} AND {3}
    AND measure_name = 'events' AND rank = 1
GROUP BY t.instance_name, t.silo, t.microservice_name
"""

##
## This query emulates an analysis scenario to find the hours of the day with highest CPU utilization
## across microservices for the specified region and cell.
##
q6Str = """
WITH per_instance_cpu_used AS (
    SELECT microservice_name, instance_name, BIN(time, 15m) AS time_bin,
        AVG(measure_value::double) AS avg_cpu
    FROM {0}.{1}
    WHERE time BETWEEN {2} AND {3}
        AND measure_name = 'cpu_user'
        AND region = '{4}'
        AND cell = '{5}'
    GROUP BY instance_name, microservice_name, BIN(time, 15m)
), per_microservice_cpu AS (
    SELECT microservice_name, HOUR(time_bin) AS hour, BIN(time_bin, 24h) AS day,
        APPROX_PERCENTILE(avg_cpu, 0.95) AS p95_avg_cpu
    FROM per_instance_cpu_used
    GROUP BY HOUR(time_bin), BIN(time_bin, 24h), microservice_name
), per_microservice_ranked AS (
    SELECT microservice_name, day, hour, p95_avg_cpu,
        DENSE_RANK() OVER (PARTITION BY microservice_name, day ORDER BY p95_avg_cpu DESC) AS rank
    FROM per_microservice_cpu
)
SELECT microservice_name, day, hour AS hour, p95_avg_cpu
FROM per_microservice_ranked
WHERE rank <= 3
ORDER BY microservice_name, day, rank ASC
"""

##
## Variant of Q5 when data is emitted in the MULTI model
##
q6StrMulti = """
WITH per_instance_cpu_used AS (
    SELECT microservice_name, instance_name, BIN(time, 15m) AS time_bin,
        AVG(cpu_user) AS avg_cpu
    FROM {0}.{1}
    WHERE time BETWEEN {2} AND {3}
        AND measure_name = 'metrics'
        AND region = '{4}'
        AND cell = '{5}'
    GROUP BY instance_name, microservice_name, BIN(time, 15m)
), per_microservice_cpu AS (
    SELECT microservice_name, HOUR(time_bin) AS hour, BIN(time_bin, 24h) AS day,
        APPROX_PERCENTILE(avg_cpu, 0.95) AS p95_avg_cpu
    FROM per_instance_cpu_used
    GROUP BY HOUR(time_bin), BIN(time_bin, 24h), microservice_name
), per_microservice_ranked AS (
    SELECT microservice_name, day, hour, p95_avg_cpu,
        DENSE_RANK() OVER (PARTITION BY microservice_name, day ORDER BY p95_avg_cpu DESC) AS rank
    FROM per_microservice_cpu
)
SELECT microservice_name, day, hour AS hour, p95_avg_cpu
FROM per_microservice_ranked
WHERE rank <= 3
ORDER BY microservice_name, day, rank ASC
"""

q1 = "do-q1"
q2 = "do-q2"
q3 = "do-q3"
q4 = "do-q4"
q5 = "do-q5"
q6 = "do-q6"

### Create the query instances based on the specified parameters.
def createQueryInstances(params, endTime = "now()", wide = False):
    if wide:
        queries = {
            ## Alerting query analyzing an hour of data.
            q1 : Query(q1StrMulti, tsb_query.QueryParams(1000, (params.dbname, params.tablename, "{} - 1h".format(endTime), "{}".format(endTime), "memory_used", params.region, params.cell, params.silo, params.az, params.microservicename))),
            ## Alerting query analyzing an hour of data.
            q2 : Query(q2StrMulti, tsb_query.QueryParams(1000, (params.dbname, params.tablename, "{} - 1h".format(endTime), "{}".format(endTime), params.region, params.cell, params.silo, params.az, params.microservicename, params.instancetype, params.osversion))),
            ## Populating a dashboard analyzing an hour of data.
            q3 : Query(q3StrMulti, tsb_query.QueryParams(50, (params.dbname, params.tablename, "{} - 1h".format(endTime), "{}".format(endTime), params.region, "disk_used", params.microservicename, 0.2))),
            ## Populating a dashboard analyzing three hours of data.
            q4 : Query(q4StrMulti, tsb_query.QueryParams(50, (params.dbname, params.tablename, "{} - 3h".format(endTime), "{}".format(endTime), params.microservicename))),
            ## Analysis query processing 1 day of data.
            q5 : Query(q5StrMulti, tsb_query.QueryParams(20, (params.dbname, params.tablename, "{} - 1d".format(endTime), "{}".format(endTime), params.region, params.cell, params.silo, params.az))),
            ## Analysis query processing 3 days of data.
            q6 : Query(q6StrMulti, tsb_query.QueryParams(20, (params.dbname, params.tablename, "{} - 3d".format(endTime), "{}".format(endTime), params.region, params.cell)))
        }
    else:
        queries = {
            ## Alerting query analyzing an hour of data.
            q1 : Query(q1Str, tsb_query.QueryParams(1000, (params.dbname, params.tablename, "{} - 1h".format(endTime), "{}".format(endTime), "memory_used", params.region, params.cell, params.silo, params.az, params.microservicename))),
            ## Alerting query analyzing an hour of data.
            q2 : Query(q2Str, tsb_query.QueryParams(1000, (params.dbname, params.tablename, "{} - 1h".format(endTime), "{}".format(endTime), params.region, params.cell, params.silo, params.az, params.microservicename, params.instancetype, params.osversion))),
            ## Populating a dashboard analyzing an hour of data.
            q3 : Query(q3Str, tsb_query.QueryParams(50, (params.dbname, params.tablename, "{} - 1h".format(endTime), "{}".format(endTime), params.region, "disk_used", params.microservicename, 0.2))),
            ## Populating a dashboard analyzing three hours of data.
            q4 : Query(q4Str, tsb_query.QueryParams(50, (params.dbname, params.tablename, "{} - 3h".format(endTime), "{}".format(endTime), params.microservicename))),
            ## Analysis query processing 1 day of data.
            q5 : Query(q5Str, tsb_query.QueryParams(20, (params.dbname, params.tablename, "{} - 1d".format(endTime), "{}".format(endTime), params.region, params.cell, params.silo, params.az))),
            ## Analysis query processing 3 days of data.
            q6 : Query(q6Str, tsb_query.QueryParams(20, (params.dbname, params.tablename, "{} - 3d".format(endTime), "{}".format(endTime), params.region, params.cell)))
        }

    return queries

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog = 'DevOps Query Driver', description='Execute the Ingestion Driver for DevOps Workload.')

    parser.add_argument('--database-name', '-d', dest="databaseName", action = "store", required = True, help = "The database name for the workload.")
    parser.add_argument('--table-name', '-t', dest="tableName", action = "store", required = True, help = "The table name for the workload.")
    parser.add_argument('--region', '-r', action = "store", required = True, help="Specify the region where the Timestream database is located.")
    parser.add_argument('--endpoint', '-e', action = "store", default = None, help="Specify the endpoint where the Timestream database is located.")
    parser.add_argument('--config', action = "store", type = str, required = True, help = "A configuration file defining properties of the workload")
    parser.add_argument('--concurrency', '-c', action = "store", type = int, default = 1, help = "Number of concurrent threads to use (default: 1)")
    parser.add_argument('--processes', '-p', action = "store", type = int, default = 1, help = "Number of concurrent processes to use (default: 1)")
    parser.add_argument('--log-dir', '-l', dest="logDir", action = "store", default = str(os.path.join(Path.home(), 'timestream_perf')), help = "The directory to log experiment results (default: ~/timestream_perf)")
    parser.add_argument('--run-prefix', dest = "runPrefix", action = "store", default = None, help = "Identifier for the run.")
    parser.add_argument('--query-end-time', dest = "queryEndTime", action = "store", default = "now()", help = "The interval end time for the query time predicates (default: 'now()')")
    parser.add_argument('--repetitions', action = "store", type = int, default = 0, help = "Whether to override the repetitions (default: 0)")
    parser.add_argument('--multi', dest = "wide", action = "store_true", help = "Enable queries in the wide format.")
    parser.add_argument('--profile', action = "store", type = str, default= None, help = "The AWS profile to use.")
    parser.add_argument('--think-time-milliseconds', dest = "thinkTimeMillis", action = "store", type = int, default = 30000, help = "Think time (in ms) between queries (default: 30000)")
    parser.add_argument('--randomized-think-time', dest = "randomizedThink", action = "store_true", help = "Use a randomized think time")
    parser.add_argument('--fixed-params', dest = "fixedParams", action = "store_true", help = "Whether to use fixed parameters for queries or get the query parameters dynamically from database.")

    args = parser.parse_args()
    print(args)

    if args.runPrefix == None:
        args.runPrefix = args.endpoint

    workloadStart = timer()
    startTime = datetime.datetime.utcnow()
    processes = list()
    for processId in range(1, args.processes + 1):
        parentConn, childConn = multiprocessing.Pipe()
        process = tsb_query.MultiProcessQueryWorker(processId, args, startTime, createQueryInstances, childConn)
        process.start()
        processes.append((process, parentConn))

    queryCount = 0
    outputs = dict()
    queryCount = 0
    outputs = dict()
    for p, conn in processes:
        output = conn.recv()
        p.join()
        if "Outputs" in output:
            outputs[p.processId] = output["Outputs"]
            queryCount += output["Count"]
        else:
            print("Process {} exited with error.".format(p.processId))
            outputs[p.processId] = dict()

    workloadEnd = timer()

    print("Experiment {} complete".format(args.runPrefix))
    for pidVals in outputs.values():
        for tidVals in pidVals.values():
            for ops in tidVals:
                print(ops)

    tps = queryCount / (workloadEnd - workloadStart)
    print("TPS: {}".format(round(tps, 3)))
