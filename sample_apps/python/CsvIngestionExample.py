#!/usr/bin/python

import csv
import time
from Constant import DATABASE_NAME, TABLE_NAME

class CsvIngestionExample:
    def __init__(self, client):
        self.client = client

    def bulk_write_records(self, filepath):
        with open(filepath, 'r') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)

            records = []
            current_time = self.__current_milli_time()

            counter = 0

            # extracting each data row one by one
            for row in csvreader:
                dimensions = [
                    {'Name': row[0], 'Value': row[1]},
                    {'Name': row[2], 'Value': row[3]},
                    {'Name': row[4], 'Value': row[5]}
                ]

                record_time = current_time - (counter * 50)

                record = {
                    'Dimensions': dimensions,
                    'MeasureName': row[6],
                    'MeasureValue': row[7],
                    'MeasureValueType': row[8],
                    'Time': str(record_time)
                }

                records.append(record)
                counter = counter + 1

                if len(records) == 100:
                    self.__submit_batch(records, counter)
                    records = []

            if len(records) != 0:
                self.__submit_batch(records, counter)

            print("Ingested %d records" % counter)


    def __submit_batch(self, records, counter):
        try:
            result = self.client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                               Records=records, CommonAttributes={})
            print("Processed [%d] records. WriteRecords Status: [%s]" % (counter, result['ResponseMetadata']['HTTPStatusCode']))
        except Exception as err:
            print("Error:", err)


    def __current_milli_time(self):
        return int(round(time.time() * 1000))
