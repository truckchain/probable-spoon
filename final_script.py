
# coding: utf-8

# ## Data aggregation
# This script does the data aggregation.
#
# Inputs: data in json format
#
# Output: Aggregated data in json

# ## Import the data

import json
import numpy as np
import datetime
from blockChain_contract import connect_to_chain
import time
LIGHT_THRESHOLD = 2000

## To add a block to the blockChain just do this:
def add_blk_light(w3, contract_instance, trip_id,timestampVal, lightVal):
    print(trip_id,timestampVal,int(lightVal))
    r  = contract_instance.transact().trackLightEvent(trip_id,timestampVal,int(lightVal))


def add_blk_bump(w3, contract_instance,trip_id,timestampVal, accVal):
    print(trip_id,timestampVal,int(abs(accVal*100000)))
    r = contract_instance.transact().trackBumpEvent(trip_id,timestampVal,int(abs(accVal*100000)))


# ## Data info
# * sensorType
# * valueLength
# * values
# * timestamp
# * sensorLocation

# ## Data pre processing

allData = []
for line in open('./data/trailer-D.json', 'r'):
    parsed_json = json.loads(line)
    allData.append(parsed_json)


def getTripid(trip_filter):
    trip_id_list = []

    for trip_dict in trip_filter:
        trip_id_list.append(trip_dict['args']['tripID'])

    return trip_id_list[-1]

assert allData != []

addr = "0x9f475A85E53A5025053c7654255172f1BE5eAda1"

## Connect to the block chain
w3, contract_instance = connect_to_chain(addr)


new_trip_filter = contract_instance.on('NewTripRegistered', {})

## Start a new trip
transaction_id = contract_instance.transact().newTrip(0,0)
time.sleep(20)
#trip_id = contract_instance.newTrip(0,0)
print(transaction_id)
trip_filter = new_trip_filter.get()

trip_id = getTripid(trip_filter)

# print(new_trip_filter.watch(call_back))
print("Trip id: ",str(trip_id))


sensors = set()
for data in allData:
    aa = data['sensorType']
    sensors.update(set([aa]))
    # print(aa)
print("Sensor types in the data", sensors)

class SensorDataALL(object):
    sensor = ""
    # The class "constructor" - It's actually an initializer
    def __init__(self, sensor):
        self.sensor = sensor

class SensorDataInfo(object):
    valueLength = None
    values = 0
    # dt = datetime.datetime(2012, 5, 1)
    timestamp = datetime.datetime.now()
    sensorLocation = ""

    # The class "constructor" - It's actually an initializer
    def __init__(self, valueLength, values, timestamp, sensorLocation, intTimeStamp):
        self.valueLength = valueLength
        self.values = values
        self.timestamp = timestamp
        self.sensorLocation = sensorLocation
        self.intTimeStamp = intTimeStamp

    def getvalues(self):
        return self.values

    def getvalueLength(self):
        return self.valueLength

    def gettimestamp(self):
        return self.timestamp

    def getsensorLocation(self):
        return self.sensorLocation

    def getintTimeStamp(self):
        return self.intTimeStamp

sensor_data = dict()
for sensor in sensors:
    sensor_data[sensor]=[]
    for data in allData:
        if data['sensorType'] ==sensor:
            timestamp1 =datetime.datetime.fromtimestamp(int(data['timestamp'])/ 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
            sensr_obj = SensorDataInfo(data['valueLength'],data['values'],timestamp1,data['sensorLocation'],data['timestamp'])
            sensor_data[sensor].append(sensr_obj)



#########################################################
########### Data preprocesssing finished!!!##############
#########################################################
## Now we run the truck data as a simulation in real time

# 1. Get the time data as a difference
light_ = []
acc_x = []
acc_y = []
acc_z = []
time_stamp = []
intTimeStamp = []
for sens,data in sensor_data.items():
    if sens=="ACCELEROMETER":
        for dat in data:
            acc_x.append(dat.getvalues()[0])
            acc_y.append(dat.getvalues()[1])
            acc_z.append(dat.getvalues()[2])
            intTimeStamp.append(dat.getintTimeStamp())
            time_stamp.append(np.datetime64(dat.gettimestamp()))
    if sens=="LIGHT":
        for dat in data:

            light_.append(dat.getvalues())

try:
    assert len(light_) == len(time_stamp)
    assert len(acc_z) == len(time_stamp)

except:
    print("Length of time stamp does not match that of sensor data")

time_diff = np.diff(time_stamp)/np.timedelta64(1, 's')

try:
    ## Run the truck data as simulation
    for i in range(len(acc_z)):
        #Data anomalies
        if light_[i]>LIGHT_THRESHOLD:
            print("Light threshold anomaly detected")
            print(time_stamp[i])
            print(intTimeStamp[i])
            add_blk_light(w3,contract_instance,trip_id,intTimeStamp[i], light_[i])

        if acc_z[i]>-0.5:
            print("Acceleration data anomaly detected")
            add_blk_bump(w3,contract_instance,trip_id,intTimeStamp[i], acc_z[i])
        print("Sleeping for time: ",time_diff[i])
        time.sleep(time_diff[i])

except:
    pass

contract_instance.transact().finalizeTrip(trip_id)
