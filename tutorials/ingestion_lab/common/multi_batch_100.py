#! /usr/bin/python3

'''This module is used to import data from alldata_skab.csv using 8 threads
with a batch size of 100 into Timestream.'''

# Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.


import csv
import multiprocessing
from datetime import datetime, timedelta
import boto3
from botocore.config import Config

start_time = datetime.now()
origin_time = int((start_time - timedelta(hours=20, minutes=00)).timestamp()*1000)

class Worker():
    '''This class is responsible for processing the alldata_skab.csv and
    importing it to Timestream'''
    def __init__(self, rows):
        self.rows = rows
        self.records = []
        self.DATABASE_NAME = "demo"
        self.TABLE_NAME = "Ingestion_Demo_multi"
        self.session = boto3.Session()
        self.client = self.session.client('timestream-write', config=Config(read_timeout=20,
                                                                    max_pool_connections=5000,
                                                                    retries={'max_attempts': 10}))
        self.run()

    def run(self):
        '''This functions starts the processing withing the thread, reads the incoming records,
        and generates timestamps starting 20 hours in the past.'''
        print("started {}".format(multiprocessing.current_process().name))
        increment = 0
        for row in self.rows:
            increment += 1000
            accelerometer_1_rms = self.create_measurement("Accelerometer1RMS",
                                                            row['Accelerometer1RMS'],
                                                            origin_time + increment)
            self.create_record(accelerometer_1_rms)

            accelerometer_2_rms = self.create_measurement("Accelerometer2RMS",
                                                            row['Accelerometer2RMS'],
                                                            origin_time + increment)
            self.create_record(accelerometer_2_rms)

            current = self.create_measurement("Current",
                                                row['Current'],
                                                origin_time + increment)
            self.create_record(current)

            pressure = self.create_measurement("Pressure",
                                                row['Pressure'],
                                                origin_time + increment)
            self.create_record(pressure)

            temperature = self.create_measurement("Temperature",
                                                    row['Temperature'],
                                                    origin_time + increment)
            self.create_record(temperature)

            thermocouple = self.create_measurement("Thermocouple",
                                                    row['Thermocouple'],
                                                    origin_time + increment)
            self.create_record(thermocouple)

            volume_flow_rate_rms = self.create_measurement("Volume Flow RateRMS",
                                                            row['Volume Flow RateRMS'],
                                                            origin_time + increment)
            self.create_record(volume_flow_rate_rms)

    def upload_record(self, record_s):
        '''This function takes in a record and uploads the record to Timestream.
        Note the Common attribute setting for the Sensor Value'''
        try:
            result = self.client.write_records(DatabaseName=self.DATABASE_NAME,
                                                TableName=self.TABLE_NAME,
                                                Records=record_s,
                                                CommonAttributes={"Dimensions":
                                                                [{"Name":"Sensor","Value":"1"}]})
        except self.client.exceptions.RejectedRecordsException as err:
            print("RejectedRecords: ", err)
            for rr in err.response["RejectedRecords"]:
                print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
                print("Other records were written successfully. ")
        except Exception as e:
            print(e)

    def create_record(self, measurement):
        '''This function combines the measure with the dimensions to
        upload into Timestream'''
        dimensions = []
        measurement['Dimensions'] = dimensions
        self.records.append(measurement)
        if len(self.records) == 100:
            self.upload_record(self.records)
            self.records = []

    def create_measurement(self, metric_name, metric, m_time):
        '''This function formats the measurement into the style needed to import
        into Timestream'''
        result = {}
        result["MeasureName"] = metric_name
        result["MeasureValue"] = metric
        result["MeasureValueType"] = "DOUBLE"
        result["Time"] = str(m_time)
        result["TimeUnit"] = "MILLISECONDS"
        return result


def start():
    '''This creates the workers for importing the data, starts them, and joins
    them so the main process doesnt quit before all the workers quit'''
    worker1 = []
    worker2 = []
    worker3 = []
    worker4 = []
    worker5 = []
    worker6 = []
    worker7 = []
    worker8 = []

    f = open('alldata_skab.csv', 'r')
    with f:
        reader = csv.DictReader(f)
        x = 1
        for row in reader:
            if x == 1:
                worker1.append(row)
            if x == 2:
                worker2.append(row)
            if x == 3:
                worker3.append(row)
            if x == 4:
                worker4.append(row)
            if x == 5:
                worker5.append(row)
            if x == 6:
                worker6.append(row)
            if x == 7:
                worker7.append(row)
            if x == 8:
                worker8.append(row)
    worker_1 = multiprocessing.Process(name='worker1', target=Worker, args=(worker1, ))
    worker_2 = multiprocessing.Process(name='worker2', target=Worker, args=(worker2, ))
    worker_3 = multiprocessing.Process(name='worker3', target=Worker, args=(worker3, ))
    worker_4 = multiprocessing.Process(name='worker4', target=Worker, args=(worker4, ))
    worker_5 = multiprocessing.Process(name='worker5', target=Worker, args=(worker5, ))
    worker_6 = multiprocessing.Process(name='worker6', target=Worker, args=(worker6, ))
    worker_7 = multiprocessing.Process(name='worker7', target=Worker, args=(worker7, ))
    worker_8 = multiprocessing.Process(name='worker8', target=Worker, args=(worker8, ))
    worker_1.start()
    worker_2.start()
    worker_3.start()
    worker_4.start()
    worker_5.start()
    worker_6.start()
    worker_7.start()
    worker_8.start()
    worker_1.join()
    worker_2.join()
    worker_3.join()
    worker_4.join()
    worker_5.join()
    worker_6.join()
    worker_7.join()
    worker_8.join()

    end_time = datetime.now()

    processing_time = end_time - start_time
    print(processing_time)


if __name__ == "__main__":
    start()
