#!/usr/bin/env python3

#
# sensordata.py
#

#
# import useful stuff
#
import boto3
import datetime
import json
import logging
import random
import threading
import time

#
# globals
#
TOPIC_BASE = 'dt/sensor'
C_IOT_DATA = boto3.client('iot-data', region_name='us-east-1')

SENSORS = {
    'sensor_01': {'building': 'Day 1', 'room': '2.01'},
    'sensor_02': {'building': 'Day 1', 'room': '10.01'},
    'sensor_03': {'building': 'Day 1', 'room': '11.02'},
    'sensor_04': {'building': 'Kumo', 'room': '12.12'},
    'sensor_05': {'building': 'Kumo', 'room': '15.07'},
    'sensor_06': {'building': 'Kumo', 'room': '00.22'},
    'sensor_07': {'building': 'Doppler', 'room': '14.10'},
    'sensor_08': {'building': 'Doppler', 'room': '15.11'},
    'sensor_09': {'building': 'Doppler', 'room': '16.12'},
    'sensor_10': {'building': 'Doppler', 'room': '17.14'}
}

#
# Configure logging
#
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.INFO)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(funcName)s - %(message)s")
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)


def sensor_data():
    message = {}
    message['temperature'] = random.uniform(15,35)
    message['pressure'] = random.uniform(30,70)
    message['humidity'] = random.uniform(900,1150)

    return message

def send_sensor_data(sensor):
    while True:
        try:
            message = sensor_data()
        
            message['device_id'] = sensor
            message['building'] = SENSORS[sensor]['building']
            message['room'] = SENSORS[sensor]['room']
    
            topic = '{}/{}'.format(TOPIC_BASE, sensor)
            logger.info("publish: topic: {} message: {}".format(topic, message))
            
            response = C_IOT_DATA.publish(topic=topic, qos=0, payload=json.dumps(message))
            logger.info("response: {}".format(response))
        except Exception as e:
            logger.error("{}".format(e))
    
        time.sleep(2)


for sensor in SENSORS.keys():
    logger.info("starting thread for sensor: {}".format(sensor))
    threading.Thread(target=send_sensor_data,args=(sensor,)).start()


start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")   
while True:
    logger.info("{}: start_time: {} now: {} threads:".format(__file__, start_time, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    for t in threading.enumerate():
        logger.info("  {}".format(t))
        
    time.sleep(30)
    
