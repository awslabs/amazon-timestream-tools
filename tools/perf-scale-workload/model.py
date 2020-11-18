from collections import defaultdict, namedtuple
import random, string
import os
import numpy as np

######################################################################################
## Data model for an example DevOps application tracking resource utilization stats ##
## for hosts deployed in a service. The service is deployed across multiple regions ##
## where each region has multiple instances of the application across different     ##
## silos and cells. The service also has multiple micro-services.                   ##
######################################################################################


######################################################################################
## A visual representation of the schema corresponding to the application.
##
##                                   region
##                             /    ...     \
##                            /     ...      \
##                          cell1           cell2
##                          /...\           /...\
##                         / ... \         / ... \
##                       silo1  silo2
##                     /  ... \
##                    /   ...  \
## availability_zone, microservice_name, instance_type, os_version, instance_name
######################################################################################

#######################################
###### Dimension model for schema #####
#######################################

regionIad = "us-east-1"
regionCmh = "us-east-2"
regionSfo = "us-west-1"
regionPdx = "us-west-2"
regionDub = "eu-west-1"
regionNrt = "ap-northeast-1"
regions = [regionIad, regionCmh, regionSfo, regionPdx, regionDub, regionNrt]
cellsPerRegion = {
    regionIad : 15, regionCmh : 2, regionSfo: 6, regionPdx : 2, regionDub : 10, regionNrt: 5
}
siloPerCell = {
    regionIad : 3, regionCmh: 2, regionSfo: 2, regionPdx: 2, regionDub : 2, regionNrt: 3
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

######################################################################################
## The metrics and event names reported by the application. These metric names are
## mapped to measure_name in Timestream.
######################################################################################
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

DimensionsMetric = namedtuple('DimensionsMetric', 'region cell silo availability_zone microservice_name instance_type os_version instance_name')
DimensionsEvent = namedtuple('DimensionsEvent', 'region cell silo availability_zone microservice_name instance_name process_name, jdk_version')

## Generate an alphanumeric string which is used as part of instance names.
def generateRandomAlphaNumericString(length = 5, seed=12345):
    ## Use a fixed seed to generate the same string across invocations.
    rand = random.Random(seed)
    x = ''.join(rand.choices(string.ascii_letters + string.digits, k=length))
    print("Instance name suffix:", x)
    return x

## Generate the values of the dimensions based on the hierarchical schema and data distribution
## characteristics defined earlier.
def generateDimensions(scaleFactor, seed = 12345):
    instancePrefix = generateRandomAlphaNumericString(8, seed)
    dimensionsMetrics = list()
    dimenstionsEvents = list()

    for region in regions:
        cellsForRegion = cellsPerRegion[region]
        siloForRegion = siloPerCell[region]
        for cell in range(1, cellsForRegion + 1):
            for silo in range(1, siloForRegion + 1):
                for microservice in microservices:
                    cellName = "{}-cell-{}".format(region, cell)
                    siloName = "{}-cell-{}-silo-{}".format(region, cell, silo)
                    numInstances = scaleFactor * instancesForMicroservice[microservice]
                    for instance in range(numInstances):
                        az = "{}-{}".format(region, (instance % 3) + 1)
                        instanceName = "i-{}-{}-{}-{:08}.amazonaws.com".format(instancePrefix, microservice, siloName, instance)
                        instanceType = instanceTypes[microservice]
                        osVersion = osVersions[microservice]
                        metric = DimensionsMetric(region, cellName, siloName, az, microservice, instanceType, osVersion, instanceName)
                        dimensionsMetrics.append(metric)

                        jdkVersion = jdkVersions[microservice]
                        for process in processNames[microservice]:
                            event = DimensionsEvent(region, cellName, siloName, az, microservice, instanceName, process, jdkVersion)
                            dimenstionsEvents.append(event)

    return (dimensionsMetrics, dimenstionsEvents)

def createWriteRecordCommonAttributes(dimensions):
    return { "Dimensions": [{ "Name": dimName, "Value": getattr(dimensions, dimName), "DimensionValueType": "VARCHAR"} for dimName in dimensions._fields] }

def createRandomMetrics(hostId, timestamp, timeUnit, highUtilizationHosts, lowUtilizationHosts):
    records = list()

    ## CPU measures
    if hostId in highUtilizationHosts:
        cpuUser = 85.0 + 10.0 * random.random()
    elif hostId in lowUtilizationHosts:
        cpuUser = 10.0 * random.random()
    else:
        cpuUser = 35.0 + 30.0 * random.random()

    records.append(createRecord(measureCpuUser, cpuUser, "DOUBLE", timestamp, timeUnit))

    otherCpuMeasures = [measureCpuSystem, measureCpuSteal, measureCpuIowait, measureCpuNice, measureCpuHi, measureCpuSi]
    totalOtherUsage = 0.0

    for measure in otherCpuMeasures:
        value = random.random()
        totalOtherUsage += value
        records.append(createRecord(measure, value, "DOUBLE", timestamp, timeUnit))

    cpuIdle = 100 - cpuUser - totalOtherUsage
    records.append(createRecord(measureCpuIdle, cpuIdle, "DOUBLE", timestamp, timeUnit))

    remainingMeasures = [measureMemoryFree, measureMemoryUsed, measureMemoryCached, measureDiskIoReads,
                        meausreDiskIoWrites, measureLatencyPerRead, measureLatencyPerWrite, measureNetworkBytesIn,
                        measureNetworkBytesOut, measureDiskUsed, measureDiskFree, measureFileDescriptors]

    for measure in remainingMeasures:
        value = 100.0 * random.random()
        records.append(createRecord(measure, value, "DOUBLE", timestamp, timeUnit))

    return records

def createRandomEvent(timestamp, timeUnit):
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


def printModelSummary(dimensionsMetrics, dimensionsEvents, metricInterval, eventInterval):
    print("Dimensions for metrics: {:,}".format(len(dimensionsMetrics)))
    print("Dimensions for events: {:,}".format(len(dimensionsEvents)))
    numTimeseries = len(dimensionsMetrics) * len(measuresForMetrics) + len(dimensionsEvents) * len(measuresForEvents)
    numDataPointsPerSecond = round((1 / metricInterval) * len(dimensionsMetrics) * len(measuresForMetrics) + (1 / eventInterval) * len(dimensionsEvents) * len(measuresForEvents))
    numDataPointsPerHour = 3600 * numDataPointsPerSecond
    avgMeasureNameLength = np.average([len(x) for x in measuresForMetrics] + [len(x) for x in measuresForEvents])
    avgDimensionsSize = np.average([np.sum([len(getattr(dimensionsEvents[0], dimName)) * 2 for dimName in dimensionsEvents[0]._fields]),
        np.sum([len(getattr(dimensionsMetrics[0], dimName)) * 2 for dimName in dimensionsMetrics[0]._fields])])
    avgRowSize = avgDimensionsSize + avgMeasureNameLength + 16

    numMetricsPerSecond = round((1 / metricInterval) * len(dimensionsMetrics) * len(measuresForMetrics))
    numEventsPerSecond = round((1 / eventInterval) * len(dimensionsEvents) * len(measuresForEvents))
    metricsMeasureBytes = np.average([len(x) for x in measuresForMetrics])
    eventsMeasureBytes = np.average([len(x) for x in measuresForEvents])
    eventsDimensionBytes = np.sum([len(getattr(dimensionsEvents[0], dimName)) * 2 for dimName in dimensionsEvents[0]._fields])
    metricsDimensionBytes = np.sum([len(getattr(dimensionsMetrics[0], dimName)) * 2 for dimName in dimensionsMetrics[0]._fields])
    ingestionVolume = round((numMetricsPerSecond * (metricsDimensionBytes + metricsMeasureBytes + 16) + numEventsPerSecond * (eventsDimensionBytes + eventsMeasureBytes + 16)) / (1024.0 * 1024.0), 2)
    dataSizePerHour = round(ingestionVolume * 3600 / 1024.0, 2)
    dataSizePerDay = round(dataSizePerHour * 24, 2)
    dataSizePerYear = round(dataSizePerDay * 365 / 1024.0, 2)

    print("avg row size: {} Bytes".format(avgRowSize))
    print("Number of timeseries: {:,}. Avg. data points per second: {:,}. Avg. data points per hour: {:,}".format(numTimeseries, numDataPointsPerSecond, numDataPointsPerHour))
    print("Avg. Ingestion volume: {:,} MB/s. Data size per hour: {:,} GB. Data size per day: {:,} GB. Data size per year: {:,} TB".format(ingestionVolume, dataSizePerHour, dataSizePerDay, dataSizePerYear))