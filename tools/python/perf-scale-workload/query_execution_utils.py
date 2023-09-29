import boto3
import json
import time
import random
import sys, traceback
from timeit import default_timer as timer
import numpy as np
from scipy.stats import gmean
import datetime
import os
from collections import defaultdict, namedtuple
import timestreamquery as timestream
import pandas as pd

Query = namedtuple('Query', 'queryStr params')
QueryResult = namedtuple('QueryResult', 'totalCount, successCount, avg, stddev, perc50, perc90, perc99, max, gmean')
QueryInstanceResult = namedtuple('QueryInstanceResult', 'result, queryId, rows, timing, success')

def executeQueryInstance(client, queryStr, execId, logPrefix = "[INFO]", thinkTimeMillis = 0.0, randomizedThink = False, outFile = None, errFile = None):
    success = 0
    rows = 0
    start = timer()
    try:
        result = timestream.executeQuery(client, queryStr, timing=True, logFile=outFile)
        success += 1

        end = timer()

        for page in result:
            if 'Rows' in page:
                rows += len(page['Rows'])

        queryId = result[0]['QueryId']

        if thinkTimeMillis > 0:
            if randomizedThink:
                thinkTimeMillis = random.randint(5000, thinkTimeMillis)
            time.sleep(thinkTimeMillis / 1000.0)
    except Exception as e:
        end = timer()
        result = None
        rows = None

        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(e)

        if errFile != None:
            errFile.write("{}\n".format(str(e)))

        if e.response != None:
            queryId = None
            if 'QueryId' in e.response:
                queryId = e.response['QueryId']
            print("QueryId: {}".format(queryId))

            if errFile != None:
                now = datetime.datetime.utcnow()
                errFile.write("{} QueryId: {} RequestId: {}\n".format(now.strftime("%Y-%m-%d %H:%M:%S"), queryId, e.response['ResponseMetadata']['RequestId']))

        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)

        if errFile != None:
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=errFile)
            errFile.write("\n")

    now = datetime.datetime.utcnow()
    output = "{} {}. {}. QueryId: {}. Time: {}. Rows: {}".format(logPrefix, execId, now.strftime("%Y-%m-%d %H:%M:%S"),
            queryId, round(end - start, 3), rows)
    print(output)

    if outFile != None:
        outFile.write("{}\n".format(output))

    return QueryInstanceResult(result, queryId, rows, (end - start), success)
