import numpy as np
from scipy.stats import gmean
from collections import namedtuple
import threading
import multiprocessing
import configparser
import timestreamquery as tsquery
import os
from timeit import default_timer as timer
from query_execution_utils import executeQueryInstance, Query
import sys, traceback
import random, string
import time

Params = namedtuple('Params', 'dbname tablename region az cell silo microservicename instancetype osversion instancename processname jdkversion')
QueryParams = namedtuple('QueryParams', 'repetitions paramlist')
Header = 'Query type, Total Count, Successful Count, Avg. latency (in secs), Std dev latency (in secs), Median, 90th perc (in secs), 99th Perc (in secs), Geo Mean (in secs)'

### Create the query string using the list of parameters.
def createQueryStr(query):
    return query.queryStr.format(*query.params.paramlist)

def getQueryStrings(queries, params):
    queryStrings = dict()
    for query in queries:
        queryStrings[query] = createQueryStr(queries[query])

    return queryStrings

## For each query, convert them into row-count variants where the actual query is enclosed within a sub-query
## where the outer query counts the number of rows returned by the sub-query (i.e., the original query).
def createQueryInstancesRowCount(queries, endTime = "now()"):
    rowCountStr = """
SELECT COUNT(*)
FROM (
    {}
)
"""
    rowCountQueries = dict()
    for key in queries:
        query = queries[key]
        rowCountQuery = Query(rowCountStr.format(query.queryStr), query.params)
        rowCountQueries[key] = rowCountQuery

    return rowCountQueries

## Config constants. These define the strings used in the config files.
configDefaultSection = 'default'
configQueryDistributionSection = 'query_distribution'
configQueryMode = 'query_mode'
configRepetitions = 'repetitions'
configRetries = 'retries'

configQueryModeRowCount = 'row_count'
configQueryModeRegular = 'regular'

class QueryStats:
    def __init__(self):
        self.count = 0
        self.success = 0
        self.timings = list()

## The main execution thread the reads in the config file and executes the queries per the parameters
## defined in the config file.
class RandomizedExecutionThread(threading.Thread):
    def __init__(self, threadId, args, startTime, queryProvider, tableName = None):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.args = args
        self.startTime = startTime
        self.config = configparser.ConfigParser()
        self.config.read(args.config)
        retries = int(self.config.get(configDefaultSection, configRetries, fallback = 0))
        self.client = tsquery.createQueryClient(region=args.region, profile=args.profile, retries=retries, endpoint=args.endpoint)
        self.output = ()
        self.tps = 0.0
        self.queryCount = 0
        self.tableName = tableName

        ## Query parameters
        if self.args.fixedParams:
            ## Use fixed query parameters
            self.params = Params(self.args.databaseName, self.args.tableName, 'eu-west-1', 'eu-west-1-1', 'eu-west-1-cell-8', 'eu-west-1-cell-8-silo-1', 'apollo', 'r5.4xlarge', 'AL2', 'i-ojtoEEXU-apollo-eu-west-1-cell-8-silo-1-00000033.amazonaws.com', 'host_manager', 'JDK_8')
        else:
            ## Read the query parameters from the database and table.
            self.params = getQueryParams(self.args)

        print(self.params)
        queryInstances = queryProvider(self.params, args.queryEndTime, args.wide)

        ## Initialize the query mode.
        self.queryMode = self.config.get(configDefaultSection, configQueryMode, fallback = configQueryModeRegular)
        if self.queryMode not in [configQueryModeRegular, configQueryModeRowCount]:
            raise Exception('Unknown query mode: {}'.format(self.queryMode))
        if self.queryMode == configQueryModeRegular:
            self.queries = getQueryStrings(queryInstances, self.params)
        else:
            queriesRowCount = createQueryInstancesRowCount(queryInstances, args.queryEndTime)
            self.queries = getQueryStrings(queriesRowCount, self.params)

        ## Initialize the query distributions.
        if configQueryDistributionSection not in self.config:
            raise Exception('Query distribution section missing')
        queryDistributions = self.config[configQueryDistributionSection]
        distributions = dict()
        for query in queryDistributions:
            if query not in self.queries:
                raise Exception('Unknown query: {}'.format(query))
            if query in distributions:
                distributions[query] += float(queryDistributions[query]) / 100
            else:
                distributions[query] = float(queryDistributions[query]) / 100

        self.queriesSelected = list()
        self.queryWeights = list()
        sum = 0
        for query in distributions.keys():
            print('{} : {}'.format(query, distributions[query]))
            self.queriesSelected.append(query)
            self.queryWeights.append(distributions[query])
            sum += distributions[query]

        print("Sum of probabilities: ", sum)

    def run(self):
        logPrefix = "[{}]".format(self.threadId)
        databaseName = self.args.databaseName
        if self.tableName == None:
            tableName = self.args.tableName
        else:
            tableName = self.tableName

        repetitions = int(self.config.get(configDefaultSection, configRepetitions, fallback = 100))

        logDir = self.args.logDir
        runPrefix = self.args.runPrefix

        if not os.path.exists(logDir):
            os.makedirs(logDir)

        ## Create an experiment name for logging purposes.
        expName = "{}-{}-{}".format(runPrefix, self.startTime.strftime("%Y-%m-%d-%H-%M-%S"), self.threadId)
        expDirName = os.path.join(logDir, expName)
        if not os.path.exists(expDirName):
            os.makedirs(expDirName)

        print("Starting experiment {} at {}. Database: {}. Table: {}. Log files at: {}".format(
            expName, self.startTime, databaseName, tableName, expDirName))
        beginExperiment = timer()

        ## Start running the experiment
        self.logStats(expDirName, logPrefix, databaseName, tableName, "begin")

        queryLogFiles = dict()
        queryExecutionStats = dict()

        try:
            ## Generate the query strings and initalize other resources
            for query in self.queriesSelected:
                outFilePath = os.path.join(expDirName, "{0}.log".format(query))
                errFilePath = os.path.join(expDirName, "{0}.err".format(query))
                sqlFilePath = os.path.join(expDirName, "{0}.sql".format(query))
                outFile = open(outFilePath, "w")
                errFile = open(errFilePath, "w")
                queryLogFiles[query] = (outFile, errFile)
                queryExecutionStats[query] = QueryStats()
                with open(sqlFilePath, "w") as file:
                    file.write(self.queries[query])

            output = list()
            output.append('Query type, Total Count, Successful Count, Avg. latency (in secs), Std dev latency (in secs), Median, 90th perc (in secs), 99th Perc (in secs), Geo Mean (in secs)')

            queryCount = 0
            while queryCount < repetitions:
                try:
                    queryCount += 1
                    ## Randomly choose a query to execute
                    queryToExecute = np.random.choice(self.queriesSelected, p=self.queryWeights)
                    queryStr = self.queries[queryToExecute]
                    print("{} {}. {}".format(logPrefix, queryCount, queryToExecute))
                    result = executeQueryInstance(self.client, queryStr, queryCount, logPrefix=logPrefix,
                                        thinkTimeMillis=self.args.thinkTimeMillis, randomizedThink = self.args.randomizedThink,
                                        outFile=queryLogFiles[queryToExecute][0], errFile=queryLogFiles[queryToExecute][1])
                    queryStat = queryExecutionStats[queryToExecute]
                    queryStat.count += 1
                    queryStat.success += result.success
                    if result.success == 1:
                        queryStat.timings.append(result.timing)
                except:
                    print("Error executing query: ", query)
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)

            print('\nSummary Results\n')
            for query in self.queriesSelected:
                queryStat = queryExecutionStats[query]
                if queryStat.success > 0:
                    output.append('{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}'.format(query, queryStat.count, queryStat.success,
                                        round(np.average(queryStat.timings), 3), round(np.std(queryStat.timings), 3),
                                        round(np.percentile(queryStat.timings, 50), 3), round(np.percentile(queryStat.timings, 90), 3),
                                        round(np.percentile(queryStat.timings, 99), 3), round(gmean(queryStat.timings), 3)))
                else:
                    output.append('{0}, {1}, {2}, , , , ,'.format(query, queryStat.count, queryStat.success))

            print(os.linesep.join("{}".format(x) for x in output))

            summaryFile = os.path.join(expDirName, "{}-summary.csv".format(expName))
            with open(summaryFile, "w") as summary:
                summary.write(os.linesep.join("{}".format(x) for x in output))

            ## Get a count at the end of the experiment.
            self.logStats(expDirName, logPrefix, databaseName, tableName, "end")

            endExperiment = timer()
            print("Experiment {} completed. Time (seconds): {}. Log directory: {}".format(expName,
                    round(endExperiment - beginExperiment, 2), expDirName))

            self.queryCount = queryCount
            self.output = output

        finally:
            for key in queryLogFiles:
                val = queryLogFiles[key]
                val[0].close()
                val[1].close()

    def getOutput(self):
        return self.output

    def getQueryCount(self):
        return self.queryCount

    def getTps(self):
        return self.tps

    ## Log a few summary statistics from the table.
    def logStats(self, expDirName, logPrefix, databaseName, tableName, stage):
        # Number of data points ingested.
        numDatapoints = """
        SELECT COUNT(*) AS num_data_points
        FROM "{0}"."{1}"
        WHERE time BETWEEN {2} AND {3}
        """
        datapoints = Query(numDatapoints, QueryParams(1, (databaseName, tableName, '{} - 15m'.format(self.args.queryEndTime), '{}'.format(self.args.queryEndTime))))
        queryStr = createQueryStr(datapoints)

        outputFilePath = os.path.join(expDirName, "numDatapoints-{}.log".format(stage))
        outFile = open(outputFilePath, "w")

        errorFilePath = os.path.join(expDirName, "numDatapoints-{}.err".format(stage))
        errFile = open(errorFilePath, "w")

        try:
            print("Obtaining the data points ingested in the last 15 mins")
            result = executeQueryInstance(self.client, queryStr, 1, logPrefix=logPrefix, outFile=outFile, errFile=errFile)
            if result != None:
                df = tsquery.flatModelToDataframe(result.result)
                print(df)
                resultFilePath = os.path.join(expDirName, "numDatapoints-{}-result.txt".format(stage))
                with open(resultFilePath, "w") as resultFile:
                    resultFile.write(str(df))
        finally:
            outFile.close()
            errFile.close()

## A multi-process executer that uses the RandomizedExecutionThread instances to execute queries
## using multiple processes.
class MultiProcessQueryWorker(multiprocessing.Process):
    def __init__(self, processId, args, startTime, queryProvider, conn):
        super(MultiProcessQueryWorker, self).__init__()
        self.processId = processId
        self.args = args
        self.conn = conn
        self.startTime = startTime
        self.queryInstanceProvider = queryProvider

    def run(self):
        threads = []
        overallSummary = dict()
        try:

            for threadId in range(1, self.args.concurrency + 1):
                threadIdStr = "{}-{}".format(self.processId, threadId)
                thread = RandomizedExecutionThread(threadIdStr, self.args, self.startTime, self.queryInstanceProvider, self.args.tableName)
                thread.start()
                if self.args.thinkTimeMillis > 0:
                    thinkTimeMillis = self.args.thinkTimeMillis
                    if self.args.randomizedThink:
                        thinkTimeMillis = random.randint(0, thinkTimeMillis)
                    time.sleep(thinkTimeMillis / 1000.0)
                threads.append(thread)

            queryCount = 0
            outputs = dict()
            for t in threads:
                t.join()
                outputs[t.threadId] = t.getOutput()
                queryCount += t.getQueryCount()

            overallSummary["Outputs"] = outputs
            overallSummary["Count"] = queryCount
            print("Experiment {} complete for process ID: {}".format(self.args.runPrefix, self.processId))
            print(os.linesep.join([os.linesep.join([x for x in z]) for z in outputs.values()]))
        finally:
            self.conn.send(overallSummary)

## Obtain the query parameters by issuing a query to the database and table.
def getQueryParams(args):
    print("Obtaining query params from the database.")
    client = tsquery.createQueryClient(region=args.region, profile=args.profile, endpoint=args.endpoint)
    ## Use the following query to get the query parameters from the data
    ## by picking attributes each type of time series to initialize the dimensions.
    ## Introduces some randomization to enable multiple executions to pick different parameters.
    if args.wide:
        queryStr = """
        WITH selectedTaskCompleted AS (
            SELECT *
            FROM "{0}"."{1}"
            WHERE measure_name = 'events'
                AND time BETWEEN {2} - 12h AND {2}
            LIMIT 1
        ), selectedCpu AS (
            SELECT t.*
            FROM "{0}"."{1}" t INNER JOIN selectedTaskCompleted c ON c.instance_name = t.instance_name
            WHERE t.measure_name = 'metrics'
                AND t.time BETWEEN {2} - 12h AND {2}
            LIMIT 1
        )
        SELECT * FROM(
            (
            SELECT * FROM selectedCpu
            )
            UNION
            (
            SELECT * FROM selectedTaskCompleted
            )
        )
        ORDER BY measure_name DESC
        """.format(args.databaseName, args.tableName, args.queryEndTime)
    else:
        queryStr = """
        WITH selectedTaskCompleted AS (
            SELECT *
            FROM "{0}"."{1}"
            WHERE measure_name = 'task_completed'
                AND time BETWEEN {2} - 12h AND {2}
            LIMIT 1
        ), selectedCpu AS (
            SELECT t.*
            FROM "{0}"."{1}" t INNER JOIN selectedTaskCompleted c ON c.instance_name = t.instance_name
            WHERE t.measure_name = 'cpu_user'
                AND t.time BETWEEN {2} - 12h AND {2}
            LIMIT 1
        )
        SELECT * FROM(
            (
            SELECT * FROM selectedCpu
            )
            UNION
            (
            SELECT * FROM selectedTaskCompleted
            )
        )
        ORDER BY measure_name
        """.format(args.databaseName, args.tableName, args.queryEndTime)
    print(queryStr)
    result = tsquery.executeQueryAndReturnAsDataframe(client, queryStr, True)
    params = Params(args.databaseName, args.tableName, result['region'][0], result['availability_zone'][0], result['cell'][0],
        result['silo'][0], result['microservice_name'][0], result['instance_type'][0], result['os_version'][0],
        result['instance_name'][0], result['process_name'][1], result['jdk_version'][1])
    return params