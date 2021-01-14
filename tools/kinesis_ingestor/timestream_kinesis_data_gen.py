import argparse
import json
import random
import signal
import string
import sys
import time
from collections import namedtuple

import boto3
import numpy as np

regions = {
    "us_east_1": dict(cells_per_region=15, silos_per_cell=3),
    "us-east-2": dict(cells_per_region=2, silos_per_cell=2),
    "us-west-1": dict(cells_per_region=6, silos_per_cell=2),
    "us-west-2": dict(cells_per_region=2, silos_per_cell=2),
    "eu-west-1": dict(cells_per_region=10, silos_per_cell=2),
    "ap-northeast-1": dict(cells_per_region=5, silos_per_cell=3),
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

measureValuesForTaskEndState = ['SUCCESS_WITH_NO_RESULT', 'SUCCESS_WITH_RESULT', 'INTERNAL_ERROR', 'USER_ERROR',
                                'UNKNOWN', 'THROTTLED']
selectionProbabilities = [0.2, 0.7, 0.01, 0.07, 0.01, 0.01]

DimensionsMetric = namedtuple('DimensionsMetric',
                              'region cell silo availability_zone microservice_name instance_type os_version instance_name')
DimensionsEvent = namedtuple('DimensionsEvent',
                             'region cell silo availability_zone microservice_name instance_name process_name, jdk_version')


utilizationRand = random.Random(12345)
lowUtilizationHosts = []
highUtilizationHosts = []

def generateRandomAlphaNumericString(length=5):
    rand = random.Random(12345)
    x = ''.join(rand.choices(string.ascii_letters + string.digits, k=length))
    print(x)
    return x


def generateDimensions(scaleFactor):
    instancePrefix = generateRandomAlphaNumericString(8)
    dimensionsMetrics = list()
    dimenstionsEvents = list()

    for region_name, region_data in regions.items():
        cellsForRegion = region_data['cells_per_region']
        siloForRegion = region_data['silos_per_cell']
        for cell in range(1, cellsForRegion + 1):
            for silo in range(1, siloForRegion + 1):
                for microservice in microservices:
                    cellName = "{}-cell-{}".format(region_name, cell)
                    siloName = "{}-cell-{}-silo-{}".format(region_name, cell, silo)
                    numInstances = scaleFactor * instancesForMicroservice[microservice]
                    for instance in range(numInstances):
                        az = "{}-{}".format(region_name, (instance % 3) + 1)
                        instanceName = "i-{}-{}-{:04}.amazonaws.com".format(instancePrefix, microservice, instance)
                        instanceType = instanceTypes[microservice]
                        osVersion = osVersions[microservice]
                        metric = DimensionsMetric(region_name, cellName, siloName, az, microservice, instanceType, osVersion,
                                                  instanceName)
                        dimensionsMetrics.append(metric)

                        jdkVersion = jdkVersions[microservice]
                        for process in processNames[microservice]:
                            event = DimensionsEvent(region_name, cellName, siloName, az, microservice, instanceName, process,
                                                    jdkVersion)
                            dimenstionsEvents.append(event)

    return (dimensionsMetrics, dimenstionsEvents)

def createRandomMetrics(host_id, timestamp, time_unit):
    records = list()

    ## CPU measures
    if host_id in highUtilizationHosts:
        cpu_user = 85.0 + 10.0 * random.random()
    elif host_id in lowUtilizationHosts:
        cpu_user = 10.0 * random.random()
    else:
        cpu_user = 35.0 + 30.0 * random.random()

    records.append(create_record(measureCpuUser, cpu_user, "DOUBLE", timestamp, time_unit))

    otherCpuMeasures = [measureCpuSystem, measureCpuSteal, measureCpuIowait, measureCpuNice, measureCpuHi, measureCpuSi]
    totalOtherUsage = 0.0

    for measure in otherCpuMeasures:
        value = random.random()
        totalOtherUsage += value
        records.append(create_record(measure, value, "DOUBLE", timestamp, time_unit))

    cpuIdle = 100 - cpu_user - totalOtherUsage
    records.append(create_record(measureCpuIdle, cpuIdle, "DOUBLE", timestamp, time_unit))

    remainingMeasures = [measureMemoryFree, measureMemoryUsed, measureMemoryCached, measureDiskIoReads,
                         meausreDiskIoWrites, measureLatencyPerRead, measureLatencyPerWrite, measureNetworkBytesIn,
                         measureNetworkBytesOut, measureDiskUsed, measureDiskFree, measureFileDescriptors]

    for measure in remainingMeasures:
        value = 100.0 * random.random()
        records.append(create_record(measure, value, "DOUBLE", timestamp, time_unit))

    return records


def createRandomEvent(timestamp, time_unit):
    records = list()

    records.append(create_record(measureTaskCompleted, random.randint(0, 500), "BIGINT", timestamp, time_unit))
    records.append(
        create_record(measureTaskEndState, np.random.choice(measureValuesForTaskEndState, p=selectionProbabilities),
                     "VARCHAR", timestamp, time_unit))

    remainingMeasures = [measureGcReclaimed, measureGcPause, measureMemoryFree]

    for measure in remainingMeasures:
        value = 100.0 * random.random()
        records.append(create_record(measure, value, "DOUBLE", timestamp, time_unit))

    return records


def create_record(measure_name, measure_value, value_type, timestamp, time_unit):
    return {
        "MeasureName": measure_name,
        "MeasureValue": str(measure_value),
        "MeasureValueType": value_type,
        "Time": str(timestamp),
        "TimeUnit": time_unit
    }


def send_records_to_kinesis(all_dimensions, kinesis_client, stream_name, sleep_time, percent_late, late_time):
    while True:
        if percent_late > 0:
            value = random.random()*100
            if (value >= percent_late):
                print("Generating On-Time Records.")
                local_timestamp = int(time.time())
            else:
                print("Generating Late Records.")
                local_timestamp = (int(time.time()) - late_time)
        else:
            local_timestamp = int(time.time())

        for series_id, dimensions in enumerate(all_dimensions):
            if isinstance(dimensions, DimensionsMetric):
                metrics = createRandomMetrics(series_id, local_timestamp, "SECONDS")
            else:
                metrics = createRandomEvent(local_timestamp, "SECONDS")

            dimensions = dimensions._asdict()  # convert named tuple to dict
            records = []
            for metric in metrics:
                metric.update(dimensions)  # adds the dimensions into metric dict
                data = json.dumps(metric)
                records.append({'Data': bytes(data, 'utf-8'), 'PartitionKey': metric['instance_name']})

            kinesis_client.put_records(StreamName=stream_name, Records=records)

            print("Wrote {} records to Kinesis Stream '{}'".format(len(metrics), stream_name))
        
        if sleep_time > 0:
            time.sleep(float(sleep_time)) 

def main(args):
    global utilizationRand
    global lowUtilizationHosts
    global highUtilizationHosts

    print(args)
    host_scale = args.hostScale  # scale factor for the hosts.

    dimension_measures, dimensions_events = generateDimensions(host_scale)

    print("Dimensions for metrics: {}".format(len(dimension_measures)))
    print("Dimensions for events: {}".format(len(dimensions_events)))

    host_ids = list(range(len(dimension_measures)))
    utilizationRand.shuffle(host_ids)
    lowUtilizationHosts = frozenset(host_ids[0:int(0.2 * len(host_ids))])
    highUtilizationHosts = frozenset(host_ids[-int(0.2 * len(host_ids)):])

    def signal_handler(sig, frame):
        print("Exiting Application")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    stream_name = args.stream
    region_name = args.region
    kinesis_client = boto3.client('kinesis', region_name=region_name)

    sleep_time = args.sleep_time
    percent_late = args.percent_late
    late_time = args.late_time

    try:
        kinesis_client.describe_stream(StreamName=stream_name)
    except:
        print("Unable to describe Kinesis Stream '{}' in region {}".format(stream_name, region_name))
        sys.exit(0)

    send_records_to_kinesis(dimension_measures + dimensions_events,
                            kinesis_client, stream_name, sleep_time, percent_late, late_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='timestream_kinesis_data_gen',
                                     description='DevOps Sample Data Generator for Timestream/KDA Sample Application.')

    parser.add_argument('--stream', action="store", type=str, default="TimestreamTestStream",
                        help="The name of Kinesis Stream.")
    parser.add_argument('--region', '-e', action="store", choices=['us-east-1', 'us-east-2', 'us-west-2', 'eu-west-1'],
                        default="us-east-1", help="Specify the region of the Kinesis Stream.")
    parser.add_argument('--host-scale', dest="hostScale", action="store", type=int, default=1,
                        help="The scale factor determines the number of hosts emitting events and metrics.")
    parser.add_argument('--profile', action="store", type=str, default=None, help="The AWS Config profile to use.")

    # Optional sleep timer to slow down data
    parser.add_argument('--sleep-time', action="store", type=int, default=0,
                        help="The amount of time in seconds to sleep between sending batches.")

    # Optional "Late" arriving data parameters
    parser.add_argument('--percent-late', action="store", type=float, default=0,
                        help="The percentage of data written that is late arriving ")
    parser.add_argument("--late-time", action="store", type=int, default=0,
                        help="The amount of time in seconds late that the data arrives")

    main(parser.parse_args())
