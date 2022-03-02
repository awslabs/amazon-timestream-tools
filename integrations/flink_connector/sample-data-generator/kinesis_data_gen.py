import argparse
import json
import random
import signal
import string
import sys
import time
from collections import namedtuple
import distutils

import boto3
import numpy as np

regions = {
    'us_east_1': dict(cells_per_region=15, silos_per_cell=3),
    'us-east-2': dict(cells_per_region=2, silos_per_cell=2),
    'us-west-1': dict(cells_per_region=6, silos_per_cell=2),
    'us-west-2': dict(cells_per_region=2, silos_per_cell=2),
    'eu-west-1': dict(cells_per_region=10, silos_per_cell=2),
    'ap-northeast-1': dict(cells_per_region=5, silos_per_cell=3),
}

microserviceApollo = 'apollo'
microserviceAthena = 'athena'
microserviceDemeter = 'demeter'
microserviceHercules = 'hercules'
microserviceZeus = 'zeus'
microservices = [microserviceApollo, microserviceAthena, microserviceDemeter, microserviceHercules, microserviceZeus]

instance_r5_4xl = 'r5.4xlarge'
instance_m5_8xl = 'm5.8xlarge'
instance_c5_16xl = 'c5.16xlarge'
instance_m5_4xl = 'm5.4xlarge'

instanceTypes = {
    microserviceApollo: instance_r5_4xl,
    microserviceAthena: instance_m5_8xl,
    microserviceDemeter: instance_c5_16xl,
    microserviceHercules: instance_r5_4xl,
    microserviceZeus: instance_m5_4xl
}

osAl2 = 'AL2'
osAl2012 = 'AL2012'

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

processHostmanager = 'host_manager'
processServer = 'server'

processNames = {
    microserviceApollo: [processServer],
    microserviceAthena: [processServer, processHostmanager],
    microserviceDemeter: [processServer, processHostmanager],
    microserviceHercules: [processServer],
    microserviceZeus: [processServer]
}

jdk8 = 'JDK_8'
jdk11 = 'JDK_11'

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

class MeasureValue:
    def __init__(self, name, value, type):
        self.name = name
        if type == 'DOUBLE':
            self.value = round(value, 2)
        else:
            self.value = value
        self.type = type


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
                    cellName = '{}-cell-{}'.format(region_name, cell)
                    siloName = '{}-cell-{}-silo-{}'.format(region_name, cell, silo)
                    numInstances = scaleFactor * instancesForMicroservice[microservice]
                    for instance in range(numInstances):
                        az = '{}-{}'.format(region_name, (instance % 3) + 1)
                        instanceName = 'i-{}-{}-{:04}.amazonaws.com'.format(instancePrefix, microservice, instance)
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

def createRandomMetrics(host_id, timestamp, time_unit, dimensions):
    measure_values = list()

    ## CPU measures
    if host_id in highUtilizationHosts:
        cpu_user = 85.0 + 10.0 * random.random()
    elif host_id in lowUtilizationHosts:
        cpu_user = 10.0 * random.random()
    else:
        cpu_user = 35.0 + 30.0 * random.random()

    measure_values.append(MeasureValue(measureCpuUser, cpu_user, 'DOUBLE'))

    otherCpuMeasures = [measureCpuSystem, measureCpuSteal, measureCpuIowait, measureCpuNice, measureCpuHi, measureCpuSi]
    totalOtherUsage = 0.0

    for measure in otherCpuMeasures:
        value = random.random()
        totalOtherUsage += value
        measure_values.append(MeasureValue(measure, value, 'DOUBLE'))

    cpuIdle = 100 - cpu_user - totalOtherUsage
    measure_values.append(MeasureValue(measureCpuIdle, round(cpuIdle, 2), 'DOUBLE'))

    remainingMeasures = [measureMemoryFree, measureMemoryUsed, measureMemoryCached, measureDiskIoReads,
                         meausreDiskIoWrites, measureLatencyPerRead, measureLatencyPerWrite, measureNetworkBytesIn,
                         measureNetworkBytesOut, measureDiskUsed, measureDiskFree, measureFileDescriptors]

    for measure in remainingMeasures:
        value = 100.0 * random.random()
        measure_values.append(MeasureValue(measure, value, 'DOUBLE'))

    return [create_record('metrics', measure_values, timestamp, dimensions)]


def createRandomEvent(timestamp, time_unit, dimensions):
    taskCompletion = MeasureValue(measureTaskCompleted, random.randint(0, 500), 'BIGINT')
    taskEndState = MeasureValue(measureTaskEndState, np.random.choice(measureValuesForTaskEndState, p=selectionProbabilities), 'VARCHAR')
    measure_values = [taskCompletion, taskEndState]

    remainingMeasures = [measureGcReclaimed, measureGcPause, measureMemoryFree]

    for measure in remainingMeasures:
        value = 100.0 * random.random()
        measure_values.append(MeasureValue(measure, value, 'DOUBLE'))

    return [create_record('events', measure_values, timestamp, dimensions)]


def create_record(object_type, measure_values, timestamp, dimensions):
    record = dict(dimensions)
    for mv in measure_values:
        record[mv.name] = mv.value
    record['time'] = timestamp
    record['@type'] = object_type
    return record


def send_records_to_kinesis(all_dimensions, kinesis_client, stream_name, sleep_time, percent_late, late_time):
    while True:
        if percent_late > 0:
            value = random.random()*100
            if (value >= percent_late):
                print('Generating On-Time Records.')
                local_timestamp = int(time.time())
            else:
                print('Generating Late Records.')
                local_timestamp = (int(time.time()) - late_time)
        else:
            local_timestamp = int(time.time())

        for series_id, dimensions in enumerate(all_dimensions):
            dimension_dict = dimensions._asdict()
            if isinstance(dimensions, DimensionsMetric):
                metrics = createRandomMetrics(series_id, local_timestamp, 'SECONDS', dimension_dict)
            else:
                metrics = createRandomEvent(local_timestamp, 'SECONDS', dimension_dict)

            records = []
            for metric in metrics:
                print(metric)
                data = json.dumps(metric)
                records.append({'Data': bytes(data, 'utf-8'), 'PartitionKey': dimension_dict['instance_name']})

            kinesis_client.put_records(StreamName=stream_name, Records=records)

            print('Wrote {} record to Kinesis Stream \'{}\''.format(
                'event' if not isinstance(dimensions, DimensionsMetric) else 'metric', stream_name))
        
        if sleep_time > 0:
            time.sleep(float(sleep_time)) 

def main(args):
    global utilizationRand
    global lowUtilizationHosts
    global highUtilizationHosts

    print(args)
    generate_events = args.generate_events
    generate_metrics = args.generate_metrics

    if not generate_events and not generate_metrics:
        print('Exiting: generate_events and generate_metrics are both false, so no data will be generated')
        sys.exit(0)

    host_scale = args.hostScale  # scale factor for the hosts.

    dimensions_measures, dimensions_events = generateDimensions(host_scale)

    if generate_metrics:
        print('Dimensions for metrics: {}'.format(len(dimensions_measures)))
    if generate_events:
        print('Dimensions for events: {}'.format(len(dimensions_events)))

    host_ids = list(range(len(dimensions_measures)))
    utilizationRand.shuffle(host_ids)
    lowUtilizationHosts = frozenset(host_ids[0:int(0.2 * len(host_ids))])
    highUtilizationHosts = frozenset(host_ids[-int(0.2 * len(host_ids)):])

    def signal_handler(sig, frame):
        print('Exiting Application')
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
        print('Unable to describe Kinesis Stream \'{}\' in region {}'.format(stream_name, region_name))
        sys.exit(0)

    dimensions = dimensions_measures if generate_metrics else []
    dimensions += dimensions_events if generate_events else []
    send_records_to_kinesis(dimensions,
                            kinesis_client, stream_name, sleep_time, percent_late, late_time)

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    if v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False

    raise argparse.ArgumentTypeError('Boolean value expected.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='timestream_kinesis_data_gen',
                                     description='DevOps Sample Data Generator for Timestream/KDA Sample Application.')

    parser.add_argument('--stream', action='store', type=str, default='TimestreamTestStream',
                        help='The name of Kinesis Stream.')
    parser.add_argument('--region', '-e', action='store', choices=['us-east-1', 'us-east-2', 'us-west-2', 'eu-west-1'],
                        default='us-east-1', help='Specify the region of the Kinesis Stream.')
    parser.add_argument('--host-scale', dest='hostScale', action='store', type=int, default=1,
                        help='The scale factor determines the number of hosts emitting events and metrics.')
    parser.add_argument('--profile', action='store', type=str, default=None, help='The AWS Config profile to use.')

    # Optional sleep timer to slow down data
    parser.add_argument('--sleep-time', action='store', type=int, default=0,
                        help='The amount of time in seconds to sleep between sending batches.')

    # Optional 'Late' arriving data parameters
    parser.add_argument('--percent-late', action='store', type=float, default=0,
                        help='The percentage of data written that is late arriving ')
    parser.add_argument('--late-time', action='store', type=int, default=0,
                        help='The amount of time in seconds late that the data arrives')

    # Optional type of data to generate
    parser.add_argument('--generate-events', action='store', type=str2bool, default=True,
                        help='Whether to generate events')
    parser.add_argument('--generate-metrics', action='store', type=str2bool, default=True,
                        help='Whether to generate metrics')

    main(parser.parse_args())
