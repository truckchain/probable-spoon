
# coding: utf-8

# ## Data aggregation
# This script does the data aggregation.
#
# Inputs: data in json format
#
# Output: Aggregated data in json

# ## Import the data

# In[1]:


import json
import numpy as np
import datetime
from blockChain_contract import deploy_contract as deploy_c
import time
LIGHT_THRESHOLD = 2000

## To add a block to the blockChain just do this:
## contract_instance.trackLightEvent(12345678,1)


def add_blk_light(tx_receipt,timestampVal, lightVal):
    old_add = tx_receipt['contractAddress']
    contract_instance.trackLightEvent(timestampVal,lightVal)
    time.sleep(30)
    print('Contract value: {}'.format(contract_instance.getTripRating()))

def add_blk_bump(tx_receipt,timestampVal, accVal):
    old_add = tx_receipt['contractAddress']
    contract_instance.trackLightEvent(timestampVal,accVal)
    time.sleep(30)
    print('Contract value: {}'.format(contract_instance.getTripRating()))
    #print(old_add)
    #while(tx_receipt['contractAddress']==old_add):
    #    print(tx_receipt['contractAddress'])
    #    time.sleep(1)


# ## Data info
# * sensorType
# * valueLength
# * values
# * timestamp
# * sensorLocation

# ## Data pre processing

# In[2]:

allData = []
for line in open('./data/trailer-D.json', 'r'):
    parsed_json = json.loads(line)
    allData.append(parsed_json)


assert allData != []

## Deploy the contract at the beginning of the trip
# contract_instance, tx_receipt = deploy_c()

# Getters + Setters for web3.eth.contract object
# print('Is truck driving: {}'.format(contract_instance.isTruckDriving()))

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
    def __init__(self, valueLength, values, timestamp, sensorLocation):
        self.valueLength = valueLength
        self.values = values
        self.timestamp = timestamp
        self.sensorLocation = sensorLocation

    def getvalues(self):
        return self.values

    def getvalueLength(self):
        return self.valueLength

    def gettimestamp(self):
        return self.timestamp

    def getsensorLocation(self):
        return self.sensorLocation



sensor_data = dict()
for sensor in sensors:
    sensor_data[sensor]=[]
    for data in allData:
        if data['sensorType'] ==sensor:
            timestamp1 =datetime.datetime.fromtimestamp(int(data['timestamp'])/ 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
            sensr_obj = SensorDataInfo(data['valueLength'],data['values'],timestamp1,data['sensorLocation'])
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
for sens,data in sensor_data.items():
    if sens=="ACCELEROMETER":
        for dat in data:
            acc_x.append(dat.getvalues()[0])
            acc_y.append(dat.getvalues()[1])
            acc_z.append(dat.getvalues()[2])
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

## Run the truck data as simulation
for i in range(len(acc_z)):
    #Data anomalies
    if light_[i]>LIGHT_THRESHOLD:
        print("Light threshold anomaly detected")
        pass
        #add_blk_light(tx_receipt,time_stamp[i], light_[i])

    if acc_z[i]>-0.5:
        print("Acceleration data anomaly detected")
        pass
        #add_blk_bump(tx_receipt,time_stamp[i], light_[i])
    print("Sleeping for time: ",time_diff[i])
    time.sleep(time_diff[i]/100)



'''

# ## Compute Veclocity

# In[10]:


## Get difference in time
# np.diff(index)/np.timedelta64(1, 's')


# In[11]:


## Define vel matrix
vel_x = []
vel_y = []
vel_z = []

acc_x = []
acc_y = []
acc_z = []
time_stp = []
vel_abs = []

## Get accelerometer data
for sens,data in sensor_data.items():
    if sens=="ACCELEROMETER":
        for dat in data:
            acc_x.append(dat.getvalues()[0])
            acc_y.append(dat.getvalues()[0])
            acc_z.append(dat.getvalues()[0])
            time_stp.append(np.datetime64(dat.gettimestamp()))

try:
    assert len(acc_x) == len(acc_y)
    assert len(acc_z) == len(time_stp)

    time_diff = np.diff(time_stp)/np.timedelta64(1, 's')

    vel_x.append(0)
    vel_y.append(0)
    vel_z.append(0)
    vel_abs.append(0)

    ## Generate velocities
    for i in range(len(acc_x)):
        # Integrate over the acceleration data:
        vx = (acc_x[i+1]-acc_x[i])*(time_diff[i]) + vel_x[i]
        vy = (acc_y[i+1]-acc_y[i])*(time_diff[i]) + vel_y[i]
        vz = (acc_z[i+1]-acc_z[i])*(time_diff[i]) + vel_z[i]
        vel_x.append(vx)
        vel_y.append(vy)
        vel_z.append(vz)
        vel_abs.append(np.sqrt(vx**2+vy**2+vz**2))

except:
    pass


# In[12]:


vel_abs


# In[13]:


from web3 import Web3, HTTPProvider, IPCProvider


# In[14]:


web3 = Web3(HTTPProvider('http://localhost:8545'))


# In[15]:


web3.eth.blockNumber


# In[ ]:



'''
