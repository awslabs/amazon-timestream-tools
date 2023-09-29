from collections import defaultdict, namedtuple
import random, string
import os
import math
import json
import time
import sys, traceback
from timeit import default_timer as timer
import numpy as np
import datetime
import threading
import argparse
from pathlib import Path
import signal
from botocore.config import Config
import boto3
import pprint

#######################################
###### Dimension model for schema #####
#######################################

regionIad = "us-east-1"
regionCmh = "us-east-2"
regionSfo = "us-west-1"
regionPdx = "us-west-2"
regionDub = "eu-west-1"
regionNrt = "ap-northeast-1"
regionFra = "eu-central-1"

regions = [regionIad, regionCmh, regionSfo, regionPdx, regionDub, regionNrt, regionFra]

cellsPerRegion = {
    regionIad : 15, regionCmh : 2, regionSfo: 6, regionPdx : 2, regionDub : 10, regionNrt: 5, regionFra: 1
}
siloPerCell = {
    regionIad : 3, regionCmh: 2, regionSfo: 2, regionPdx: 2, regionDub : 2, regionNrt: 3, regionFra: 1
}

microserviceApollo = "apollo"
microserviceAthena = "athena"
microserviceDemeter = "demeter"
microserviceHercules = "hercules"
microserviceZeus = "zeus"
microservices = [microserviceApollo, microserviceAthena, microserviceDemeter, microserviceHercules, microserviceZeus]

instance_r5_4xl = "r5.4xlarge"
instance_m5_8xl = "m5.8xlarge"
instance_c5_16xl = "c5.16xlarge"
instance_m5_4xl = "m5.4xlarge"

instanceTypes = {
    microserviceApollo: instance_r5_4xl,
    microserviceAthena: instance_m5_8xl,
    microserviceDemeter: instance_c5_16xl,
    microserviceHercules: instance_r5_4xl,
    microserviceZeus: instance_m5_4xl
}

osAl2 = "AL2"
osAl2012 = "AL2012"

osVersions = {
    microserviceApollo: osAl2,
    microserviceAthena: osAl2012,
    microserviceDemeter: osAl2012,
    microserviceHercules: osAl2012,
    microserviceZeus: osAl2
}

instancesForMicroservice = {
    microserviceApollo: 3,
    microserviceAthena: 1,
    microserviceDemeter: 1,
    microserviceHercules: 2,
    microserviceZeus: 3
}

processHostmanager = "host_manager"
processServer = "server"

processNames = {
    microserviceApollo: [processServer],
    microserviceAthena: [processServer, processHostmanager],
    microserviceDemeter: [processServer, processHostmanager],
    microserviceHercules: [processServer],
    microserviceZeus: [processServer]
}

jdk8 = "JDK_8"
jdk11 = "JDK_11"

jdkVersions = {
    microserviceApollo: jdk11,
    microserviceAthena: jdk8,
    microserviceDemeter: jdk8,
    microserviceHercules: jdk8,
    microserviceZeus: jdk11
}

measureCpuUser = 'cpu_user'
measureCpuSystem = 'cpu_system'
measureCpuIdle = 'cpu_idle'
measureCpuIowait = 'cpu_iowait'
measureCpuSteal = 'cpu_steal'
measureCpuNice = 'cpu_nice'
measureCpuSi = 'cpu_si'
measureCpuHi = 'cpu_hi'
measureMemoryFree = 'memory_free'
measureMemoryUsed = 'memory_used'
measureMemoryCached = 'memory_cached'
measureDiskIoReads = 'disk_io_reads'
meausreDiskIoWrites = 'disk_io_writes'
measureLatencyPerRead = 'latency_per_read'
measureLatencyPerWrite = 'latency_per_write'
measureNetworkBytesIn = 'network_bytes_in'
measureNetworkBytesOut = 'network_bytes_out'
measureDiskUsed = 'disk_used'
measureDiskFree = 'disk_free'
measureFileDescriptors = 'file_descriptors_in_use'

measureTaskCompleted = 'task_completed'
measureTaskEndState = 'task_end_state'
measureGcReclaimed = 'gc_reclaimed'
measureGcPause = 'gc_pause'

measuresForMetrics = [measureCpuUser, measureCpuSystem, measureCpuIdle, measureCpuIowait,
                      measureCpuSteal, measureCpuNice, measureCpuSi, measureCpuHi,
                      measureMemoryFree, measureMemoryUsed, measureMemoryCached, measureDiskIoReads,
                      meausreDiskIoWrites, measureLatencyPerRead, measureLatencyPerWrite, measureNetworkBytesIn,
                      measureNetworkBytesOut, measureDiskUsed, measureDiskFree, measureFileDescriptors]

measuresForEvents = [measureTaskCompleted, measureTaskEndState, measureGcReclaimed, measureGcPause, measureMemoryFree]

measureValuesForTaskEndState = ['SUCCESS_WITH_NO_RESULT', 'SUCCESS_WITH_RESULT', 'INTERNAL_ERROR', 'USER_ERROR', 'UNKNOWN', 'THROTTLED']
selectionProbabilities = [0.2, 0.7, 0.01, 0.07, 0.01, 0.01]

DimensionsMetric = namedtuple('DimensionsMetric', 'region cell silo availability_zone microservice_name instance_name instance_type os_version')
DimensionsEvent = namedtuple('DimensionsEvent', 'region cell silo availability_zone microservice_name instance_name process_name jdk_version')

def generateRandomAlphaNumericString(length = 5):
    rand = random.Random(12345)
    x = ''.join(rand.choice(string.ascii_letters + string.digits) for x in range(length))
    return x

def generateDimensions(regionINC, microserviceINC, scaleFactor):
    instancePrefix = generateRandomAlphaNumericString(8)
    dimensionsMetrics = list()
    dimenstionsEvents = list()
    
    myRegions = set(regions) & set(regionINC) if regionINC else regions
    myMicroservices = set(microservices) & set(microserviceINC) if microserviceINC else microservices
    
    print('Generating dimensions for regions {} and microservices {}'.format(myRegions, myMicroservices))

    for region in myRegions:
        cellsForRegion = cellsPerRegion[region]
        siloForRegion = siloPerCell[region]
        for cell in range(1, cellsForRegion + 1):
            for silo in range(1, siloForRegion + 1):
                for microservice in myMicroservices:
                    cellName = "{}-cell-{}".format(region, cell)
                    siloName = "{}-cell-{}-silo-{}".format(region, cell, silo)
                    numInstances = scaleFactor * instancesForMicroservice[microservice]
                    for instance in range(numInstances):
                        az = "{}-{}".format(region, (instance % 3) + 1)
                        instanceName = "i-{}-{}-{:04}.amazonaws.com".format(instancePrefix, microservice, instance)
                        instanceType = instanceTypes[microservice]
                        osVersion = osVersions[microservice]
                        metric = DimensionsMetric(region, cellName, siloName, az, microservice, instanceName, instanceType, osVersion)
                        dimensionsMetrics.append(metric)

                        jdkVersion = jdkVersions[microservice]
                        for process in processNames[microservice]:
                            event = DimensionsEvent(region, cellName, siloName, az, microservice, instanceName, process, jdkVersion)
                            dimenstionsEvents.append(event)

    return (dimensionsMetrics, dimenstionsEvents)


#########################################
######### Signal/Metric/Event  ##########
#########################################
def addSinSignal(hostId, timestamp, timeUnit, valueMax, sinSignalCpu, sinFrqCpu, args):
    if "m" == sinFrqCpu :    tf = 2 * math.pi / (60)
    elif "h" == sinFrqCpu :  tf = 2 * math.pi / (60 * 60)
    elif "d" == sinFrqCpu :  tf = 2 * math.pi / (60 * 60 * 24)
    elif "we" == sinFrqCpu : tf = 2 * math.pi / (60 * 60 * 24 * 7)
    elif "mo" == sinFrqCpu : tf = 2 * math.pi / (60 * 60 * 24 * 30.46)
    elif "qu" == sinFrqCpu : tf = 2 * math.pi / (60 * 60 * 24 * 30.46 * 3)
    elif "ye" == sinFrqCpu : tf = 2 * math.pi / (60 * 60 * 24 * 365.6)

    # SIN Signal: (sin(t) + 1) / 2 => sin(t) = 0..valueMax 
    sigValue = round(valueMax * ((math.sin(timestamp * tf) + 1 ) / 2) , 2) 
    # 100 is defined as no noise
    if sinSignalCpu >= 100: return sigValue

    # Noise: random 0..1 * valueMax - valueMax / 2 => -valueMax / 2 ... +valueMax /2
    valueMaxHalf = valueMax / 2
    noiseValue = valueMax * random.random() - valueMaxHalf
    
    # Mix signal with correct ratio, additive with noise 
    newValue = sigValue + (noiseValue / sinSignalCpu)
    
    # this can lead to above max / below min, so we need to cut here.
    newValue = max(min(round(newValue, 4), valueMax), 0)
    if args.dryRun:
        print("{}: {}Hz => {} + {} = {} ==> SNR {} => N={} on hostId {}" \
            .format((datetime.datetime.fromtimestamp(timestamp)).strftime("%Y-%m-%d %H:%M:%S"), \
                round(tf,4), sigValue, (noiseValue / sinSignalCpu), newValue, sinSignalCpu, noiseValue, hostId))
    return newValue

def addSawSignal(hostId, timestamp, timeUnit, valueMax, sawSignalCpu, sawFrqCpu, args):
    if "m" == sawFrqCpu :    tf =(60)
    elif "h" == sawFrqCpu :  tf =(60 * 60)
    elif "d" == sawFrqCpu :  tf =(60 * 60 * 24)
    elif "we" == sawFrqCpu : tf =(60 * 60 * 24 * 7)
    elif "mo" == sawFrqCpu : tf =(60 * 60 * 24 * 30.46)
    elif "qu" == sawFrqCpu : tf =(60 * 60 * 24 * 30.46 * 3)
    elif "ye" == sawFrqCpu : tf =(60 * 60 * 24 * 365.6)

    # SAW Signal: 0..valueMax in period. (y = k * x % tf + d)
    sigValue = round(valueMax * ((timestamp % tf) / tf ) , 2)
    # 100 is defined as no noise
    if sawSignalCpu >= 100: return sigValue

    
    # Noise: random 0..1 * valueMax - valueMax / 2 => -valueMax / 2 ... +valueMax /2
    valueMaxHalf = valueMax / 2
    noiseValue = valueMax * random.random() - valueMaxHalf
    
    # Mix signal with correct ratio, additive with noise 
    newValue = sigValue + (noiseValue / sawSignalCpu)
    
    # this can lead to above max / below min, so we need to cut here.
    newValue = max(min(round(newValue, 4), valueMax), 0)
    if args.dryRun:
        print("{}: {}Hz => {} + {} = {} ==> SNR {} => N={} on hostId {}" \
            .format((datetime.datetime.fromtimestamp(timestamp)).strftime("%Y-%m-%d %H:%M:%S"), \
                round(tf,4), sigValue, (noiseValue / sawSignalCpu), newValue, sawSignalCpu, noiseValue, hostId))
    return newValue
    
def createWriteRecordCommonAttributes(dimensions):
    return { "Dimensions": [{ "Name": dimName, "Value": getattr(dimensions, dimName), "DimensionValueType": "VARCHAR"} for dimName in dimensions._fields] }

    
def createRandomMetrics(hostId, timestamp, timeUnit, args):
    records = list()

    # CPU measures
    # sinSignalCpu, sinFrqCpu, sawSignalCpu, sawFrqCpu
    if args.sinSignalCpu > 0:
        cpuUser = addSinSignal(hostId, timestamp, timeUnit, 100.0, args.sinSignalCpu, args.sinFrqCpu, args)
    elif args.sawSignalCpu > 0:
        cpuUser = addSawSignal(hostId, timestamp, timeUnit, 100.0, args.sawSignalCpu, args.sawFrqCpu, args)
    elif hostId in highUtilizationHosts:
        cpuUser = random.gauss(85.0 , 10.0)
    elif hostId in lowUtilizationHosts:
        cpuUser = random.gauss(15.0 , 10.0)
    else :
        cpuUser = 100.0 * random.random()

    cpuUser = abs(round(min(100, cpuUser), 2))
    records.append(createRecord(measureCpuUser, cpuUser, "DOUBLE", timestamp, timeUnit))

    otherCpuMeasures = [measureCpuSystem, measureCpuSteal, measureCpuIowait, measureCpuNice, measureCpuHi, measureCpuSi]
    totalOtherUsage = 0
    availableOtherUsage = 100 - cpuUser

    for measure in otherCpuMeasures:
        value = round(random.random(),2)
        availableOtherUsage -= value
        if totalOtherUsage < 0:
            value = 0
        totalOtherUsage += value
        records.append(createRecord(measure, value, "DOUBLE", timestamp, timeUnit))

    cpuIdle = round(max([100 - cpuUser - totalOtherUsage, 0]),2)
    records.append(createRecord(measureCpuIdle, cpuIdle, "DOUBLE", timestamp, timeUnit))

    # delete cpu values when working with missing.    
    if args.missingCpu > 0:
        rnd = 100.0 * random.random()
        if args.missingCpu > rnd:
            records = list()

    # Memory metrics
    memUsed = 8000.0 * random.random()
    memFree = 8000.0 - memUsed
    records.append(createRecord(measureMemoryFree, "{}".format(memFree) , "DOUBLE", timestamp, timeUnit))
    records.append(createRecord(measureMemoryUsed, "{}".format(memUsed) , "DOUBLE", timestamp, timeUnit))
    records.append(createRecord(measureMemoryCached, "{}".format(4000.0 * random.random()) , "DOUBLE", timestamp, timeUnit))

    # Disc metrics
    discUpDown = random.gauss(5.0, 5.0)
    records.append(createRecord(measureDiskUsed, "{}".format(hostId * 30.0 + discUpDown) , "DOUBLE", timestamp, timeUnit))
    records.append(createRecord(measureDiskFree, "{}".format(15.0 - discUpDown) , "DOUBLE", timestamp, timeUnit))
    records.append(createRecord(measureFileDescriptors, "{}".format(random.gauss(2200.0, 500.0)) , "DOUBLE", timestamp, timeUnit))
    records.append(createRecord(measureDiskIoReads, "{}".format(25000.0 * random.random()) , "DOUBLE", timestamp, timeUnit))
    records.append(createRecord(meausreDiskIoWrites, "{}".format(25000.0 * random.random()) , "DOUBLE", timestamp, timeUnit))

    # Network metrics
    records.append(createRecord(measureLatencyPerRead, "{}".format(random.gauss(8.0, 4.0)) , "DOUBLE", timestamp, timeUnit))
    records.append(createRecord(measureLatencyPerWrite, "{}".format(random.gauss(12.0, 8.0)) , "DOUBLE", timestamp, timeUnit))
    records.append(createRecord(measureNetworkBytesIn, "{}".format(1000.0 * random.random()) , "DOUBLE", timestamp, timeUnit))
    records.append(createRecord(measureNetworkBytesOut, "{}".format(5000.0 * random.random()) , "DOUBLE", timestamp, timeUnit))

    return records

def createRandomEvent(hostId, timestamp, timeUnit, args):
    records = list()

    records.append(createRecord(measureTaskCompleted, random.randint(0, 500), "BIGINT", timestamp, timeUnit))
    records.append(createRecord(measureTaskEndState, np.random.choice(measureValuesForTaskEndState, p=selectionProbabilities), "VARCHAR", timestamp, timeUnit))

    remainingMeasures = [measureGcReclaimed, measureGcPause, measureMemoryFree]

    for measure in remainingMeasures:
        value = 100.0 * random.random()
        records.append(createRecord(measure, value, "DOUBLE", timestamp, timeUnit))

    return records

def createRecord(measureName, measureValue, valueType, timestamp, timeUnit):
    return {
        "MeasureName": measureName,
        "MeasureValue": str(measureValue),
        "MeasureValueType": valueType,
        "Time": str(timestamp),
        "TimeUnit": timeUnit
    }

seriesId = 0
timestamp = int(time.time())
sigInt = False
lock = threading.Lock()
iteration = 0
utilizationRand = random.Random(12345)
lowUtilizationHosts = []
highUtilizationHosts = []

def signalHandler(sig, frame):
    global sigInt
    global lock
    global sigInt

    with lock:
        sigInt = True

#########################################
######### Ingestion Thread ###############
#########################################
class IngestionThread(threading.Thread):
    def __init__(self, tsClient, threadId, args, dimensionMetrics, dimensionEvents):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.args = args
        self.dimensionMetrics = dimensionMetrics
        self.dimensionEvents = dimensionEvents
        self.client = tsClient
        self.databaseName = args.databaseName
        self.tableName = args.tableName
        self.numMetrics = len(dimensionMetrics)
        self.numEvents = len(dimensionEvents)

    def run(self):
        global seriesId
        global timestamp
        global lock
        global iteration

        timings = list()
        success = 0
        idx = 0

        metricCnt = 0
        eventCnt = 0
        recordCnt = 0

        while True:
            # get the seriesId with lock - ensure that no two thread's create records
            # for the same time series identified by an unique dimension set (see Dimensions tuple).
            with lock:
                if sigInt == True:
                    print("Thread {} exiting.".format(self.threadId))
                    break

                if seriesId >= self.numMetrics + self.numEvents:
                    seriesId = 0
                    iteration += 1
                    while (timestamp == int(time.time())): # ensure that each iteration has a "new" timestamp
                        time.sleep(0.1)
                    timestamp = int(time.time())
                    print("Resetting to first series from thread: [{}] at time {}. Timestamp set to: {}. Iteration {} finished."
                     .format(self.threadId, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), timestamp, iteration))

                localSeriesId = seriesId
                localTimestamp = timestamp
                seriesId += 1
                
            if (args.autostop > 0) and (args.autostop <= iteration):
                break

            if localSeriesId < self.numMetrics:
                commonAttributes = createWriteRecordCommonAttributes(self.dimensionMetrics[localSeriesId])
                records = createRandomMetrics(localSeriesId, localTimestamp, "SECONDS", self.args)
                metricCnt += len(records)
            else:
                commonAttributes = createWriteRecordCommonAttributes(self.dimensionEvents[localSeriesId - self.numMetrics])
                records = createRandomEvent(localSeriesId, localTimestamp, "SECONDS", self.args)
                eventCnt += len(records)

            start = timer()
            try:
                if not (args.dryRun):
                    writeResult = writeRecords(self.client, self.databaseName, self.tableName, commonAttributes, records)
                recordCnt += len(records)
                success += 1
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, limit=5, file=sys.stdout)

                print("T{:02d}: RequestId: {} for timestamp {}".format(self.threadId, e.response['ResponseMetadata']['RequestId'], localTimestamp))
                if e.response['RejectedRecords']:
                    print(json.dumps(e.response['RejectedRecords'], indent=2))

                print(json.dumps(commonAttributes, indent=2))
                print(json.dumps(records, indent=2))
                continue
            finally:
                end = timer()
                timings.append(end - start)

            if idx  % 100 == 0:
                now = datetime.datetime.now()
                print("T{:02d}: {}. Metrics/Events/Total: {}/{}/{}. Time: {}. Loop: {}"\
                      .format(self.threadId, now.strftime("%Y-%m-%d %H:%M:%S"), metricCnt, eventCnt, recordCnt, round(end - start, 3), idx))
            idx += 1

        self.success = success
        self.timings = timings


#########################################
######### Ingest load ###################
#########################################

def ingestRecords(tsClient, dimensionsMetrics, dimensionsEvents, args):
    numThreads = args.concurrency
    if numThreads > len(dimensionsMetrics):
        print("Can't have more threads than dimension metrics. Working with {} thread(s).".format(len(dimensionsMetrics)))
        numThreads = len(dimensionsMetrics)

    ingestionStart = timer()
    timings = list()
    threads = list()

    for threadId in range(numThreads):
        print("Starting ThreadId: {}".format(threadId + 1))
        thread = IngestionThread(tsClient, threadId + 1, args, dimensionsMetrics, dimensionsEvents)
        thread.start()
        threads.append(thread)

    success = 0
    for t in threads:
        t.join()
        success += t.success
        timings.extend(t.timings)

    print("Total={}, Success={}, Avg={}, Stddev={}, 50thPerc={}, 90thPerc={}, 99thPerc={}".format(len(timings), success,
                                                                                                  round(np.average(timings), 3),
                                                                                                  round(np.std(timings), 3), round(np.percentile(timings, 50), 3),
                                                                                                  round(np.percentile(timings, 90), 3), round(np.percentile(timings, 99), 3)))

    ingestionEnd = timer()
    print("Total time to ingest: {} seconds".format(round(ingestionEnd - ingestionStart, 2)))

#########################################
######### Timestream API calls ##########
#########################################
def createWriteClient(region, endpoint_url, profile = None):
    if profile == None:
        print("Using credentials from the environment")

    print("Connecting to timestream ingest in region: ", region)
    config = Config()
    if profile != None:
        session = boto3.Session(profile_name = profile)
        client = session.client(service_name = 'timestream-write',
                                region_name = region, endpoint_url=endpoint_url, config = config)
    else:
        session = boto3.Session()
        client = session.client(service_name = 'timestream-write',
                                region_name = region, endpoint_url=endpoint_url, config = config)
    return client

def describeTable(client, databaseName, tableName):
    response = client.describe_table(DatabaseName = databaseName, TableName = tableName)
    print("Table Description:")
    pprint.pprint(response['Table'])

def writeRecords(client, databaseName, tableName, commonAttributes, records):
    return client.write_records(DatabaseName = databaseName, TableName = tableName,
                                CommonAttributes = (commonAttributes), Records = (records))

#########################################
######### Main ##########
#########################################
if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog = 'TimestreamSampleContinuousDataIngestorApplication', description='Execute an application generating and ingesting time series data.')

    parser.add_argument('--region', '-r', '--endpoint', '-e', action = "store", required = True, \
                        help="Specify the service region. E.g. 'us-east-1'")
    parser.add_argument('--endpoint-url', '-url', action = "store", required = False, 
                        help="Specify the service endpoint url that you have been mapped to. E.g. 'https://ingest-cell2.timestream.us-east-1.amazonaws.com'")
    parser.add_argument('--profile', action = "store", type = str, default= None, 
                        help = "The AWS Config profile to use.")

    parser.add_argument('--database-name', '-d', dest="databaseName", action = "store", required = True, \
                        help = "The database name in Amazon Timestream - must be already created.")
    parser.add_argument('--table-name', '-t', dest="tableName", action = "store", required = True, \
                        help = "The table name in Amazon Timestream - must be already created.")

    parser.add_argument('--concurrency', '-c', action = "store", type = int, default = 10, 
                        help = "Number of concurrent ingestion threads (default: 10)")

    parser.add_argument('--host-scale',  '-s', dest = "hostScale", action = "store", type = int, default = 1, \
                        help = "The scale factor that determines the number of hosts emitting events and metrics (default: 1).")

    parser.add_argument('--include-region', dest = "includeRegion", action = "store", \
                        help = "Comma separated include of regions (default: EMPTY, all)")
    parser.add_argument('--include-ms', dest = "includeMs", action = "store", \
                        help = "Comma separated include of microservice (default: EMPTY, all).")

    parser.add_argument('--missing-cpu', dest = "missingCpu", action = "store", type = int, default = 0, \
                        help = "The percentage of missing values [0-100], (default: 0).")
    
    parser.add_argument('--sin-signal-cpu', dest = "sinSignalCpu", action = "store", type = float, default = 0, \
                        help = "The SIN signal to noise ratio, [0-100] no signal to 100 times noise, (default: 0).")
    parser.add_argument('--sin-frq-cpu', dest = "sinFrqCpu", action = "store", type = str, default = "m", \
                        help = "The SIN signal frequency (m | h | d | we | mo | qu | ye) (default:m)")
    parser.add_argument('--saw-signal-cpu', dest = "sawSignalCpu", action = "store", type = float, default = 0, \
                        help = "The SAW signal to noise ratio, [0-100] no signal to 100 times noise, (default: 0).")
    parser.add_argument('--saw-frq-cpu', dest = "sawFrqCpu", action = "store", type = str, default = "m", \
                        help = "The SIN signal frequency (m | h | d | we | mo | qu | ye) (default:m)")

    parser.add_argument('--seed', dest = "seed", action = "store", type = int, default = int(time.time()), \
                        help = "The seed with which to initialize random, (default: now()).")
    parser.add_argument('--autostop', action = "store", type = int, default = 0, \
                        help = "Stop each ingestor threads after N iterates (=values per time series) records. (default:0, unlimited)")
    parser.add_argument('--dry-run', dest = "dryRun", action = "store_true", \
                        help = "Run program without 'actually' writing data to time stream.")

    args = parser.parse_args()
    print(args)

    hostScale = args.hostScale       # scale factor for the hosts.

    regionINC = []
    if args.includeRegion is not None:
        regionINC = args.includeRegion.split(",")
    
    microserviceINC = []
    if args.includeMs is not None:
        microserviceINC = args.includeMs.split(",")

    random.seed(args.seed)
        
    dimensionsMetrics, dimensionsEvents = generateDimensions(regionINC, microserviceINC, hostScale)
    hostIds = list(range(len(dimensionsMetrics)))
    utilizationRand.shuffle(hostIds)
    lowUtilizationHosts = frozenset(hostIds[0:math.ceil(int(0.2 * len(hostIds)))])
    highUtilizationHosts = frozenset(hostIds[-int(0.2 * len(hostIds)):])

    print("{} unique metric dimensions.".format(len(dimensionsMetrics)))
    print("{} unique event dimensions.".format(len(dimensionsEvents)))

    # Register sigint handler
    signal.signal(signal.SIGINT, signalHandler)

    # Verify the table
    try:
        tsClient = any
        if not (args.dryRun):
            tsClient = createWriteClient(args.region, args.endpoint_url,  profile=args.profile)
            describeTable(tsClient, args.databaseName, args.tableName)
    except Exception as e:
        print(e)
        sys.exit(0)

    # Run the ingestion load.
    ingestRecords(tsClient, dimensionsMetrics, dimensionsEvents, args)
