#! Carrier Registry contract. Allows carriers to register their truck's IoT
#! devices, record trips, and rate trip quality based on sensor data.
#!
#! Copyright (c) 2017 Mayur Andulkar
#!
#! Permission is hereby granted, free of charge, to any person obtaining a copy
#! of this software and associated documentation files (the "Software"), to deal
#! in the Software without restriction, including without limitation the rights
#! to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#! copies of the Software, and to permit persons to whom the Software is
#! furnished to do so, subject to the following conditions:
#
#! The above copyright notice and this permission notice shall be included in all
#! copies or substantial portions of the Software.
#!
#! THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#! IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#! FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#! AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#! LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#! OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#! SOFTWARE.



# ## Data aggregation
# This script does the data aggregation.
#
# Inputs: data in json format
#
# Output: Runs a simulation of the real data based on the time internval and
# makes trasactions on the blockchain for changes in the light intensity and ACCELEROMETER data.

import json
import numpy as np
import datetime
print(str(np.datetime64(datetime.datetime.now()))+" Probable spoon simulation started")
from blockChain_contract import connect_to_chain
import time
LIGHT_THRESHOLD = 500
ACC_THRESHOLD = -0.5


## To add a block to the blockChain just do this:
def add_blk_light(w3, contract_instance, trip_id,timestampVal, lightVal):
    r  = contract_instance.transact().trackLightEvent(trip_id,timestampVal,int(lightVal))


def add_blk_bump(w3, contract_instance,trip_id,timestampVal, accVal):
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


## Connect to a block chain and the get the trip id based on event trigger

addr = "0x9f475A85E53A5025053c7654255172f1BE5eAda1"

## Connect to the block chain
w3, contract_instance = connect_to_chain(addr)


new_trip_filter = contract_instance.on('NewTripRegistered',{})

## Start a new trip
transaction_id = contract_instance.transact().newTrip(0,0)
time.sleep(20)
trip_filter = new_trip_filter.get()

trip_id = getTripid(trip_filter)

print(str(np.datetime64(datetime.datetime.now()))+" Trip id: "+str(trip_id)+" registered for "+addr)

## Preprocess the sensor data
sensors = set()
for data in allData:
    aa = data['sensorType']
    sensors.update(set([aa]))
    # print(aa)
# print("Sensor types in the data", sensors)

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
########### Data preprocesssing done!!!##############
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
    pass
    # print(str(np.datetime64(datetime.datetime.now()))+" Length of time stamp does not match that of sensor data")

time_diff = np.diff(time_stamp)/np.timedelta64(1, 's')

print(str(np.datetime64(datetime.datetime.now()))+" Simulator started")

try:
    ## Run the truck data as simulation
    for i in range(1,len(acc_z)):
        #Data anomalies
        if abs(light_[i]-light_[i-1])>LIGHT_THRESHOLD:
            print(str(np.datetime64(datetime.datetime.now()))+" The state of the light was changed")
            add_blk_light(w3,contract_instance,trip_id,intTimeStamp[i], light_[i])

        # ACC_THRESHOLD is absolute. If bumpy road occurs, then perform a commit on the blockChain
        if acc_z[i]>ACC_THRESHOLD:
            print(str(np.datetime64(datetime.datetime.now()))+" Tracking bumpy road")
            add_blk_bump(w3,contract_instance,trip_id,intTimeStamp[i], acc_z[i])

        else:
            print(str(np.datetime64(datetime.datetime.now()))+" No notable change detected")
        # print("Sleeping for time: ",time_diff[i])
        time.sleep(time_diff[i]/100)

except:
    pass

print(str(np.datetime64(datetime.datetime.now()))+" End of trip")
contract_instance.transact().finalizeTrip(trip_id)
