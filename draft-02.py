
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


# In[3]:


sensors = set()
for data in allData:
    aa = data['sensorType']
    sensors.update(set([aa]))
    # print(aa)
print(sensors)


# In[4]:


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


# In[5]:


sensor_data = dict()
for sensor in sensors:
    sensor_data[sensor]=[]
    for data in allData:
        if data['sensorType'] ==sensor:
            timestamp1 =datetime.datetime.fromtimestamp(int(data['timestamp'])/ 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
            sensr_obj = SensorDataInfo(data['valueLength'],data['values'],timestamp1,data['sensorLocation'])
            sensor_data[sensor].append(sensr_obj)


# In[6]:


for sens,data in sensor_data.items():
    print(sens)
    # print(data.gettimestamp())
    for dat in data:
        print(np.datetime64(dat.gettimestamp()))
        #time_stamp = datetime.datetime.fromtimestamp().strftime('%Y-%m-%d %H:%M:%S')
        # print(dat.getvalues()[0], dat.gettimestamp())


# ## Plotting the data

# In[7]:


type(datetime.datetime.now())


# In[8]:


import matplotlib.pyplot as plt

time_stp = []
sens_val = []

## Generate the data list
for sens,data in sensor_data.items():
    if sens=="GYROSCOPE":
        for dat in data:
            sens_val.append(dat.getvalues()[0])
            time_stp.append(dat.gettimestamp())

x = [datetime.datetime.strptime(elem, '%Y-%m-%d %H:%M:%S.%f') for elem in time_stp]
            
print(len(x),len(sens_val))
# Plot the data
plt.plot(x,sens_val, label='linear')
# Add a legend
plt.legend()
# Show the plot
plt.show()


# In[9]:


import matplotlib.pyplot as plt

time_stp = []
sens_val = []

## Generate the data list
for sens,data in sensor_data.items():
    if sens=="LIGHT":
        for dat in data:
            sens_val.append(dat.getvalues())
            time_stp.append(dat.gettimestamp())

            
## Generate the data list
sens_val1 = []
sens_val2 = []
for sens,data in sensor_data.items():
    if sens=="TEMPERATURE":
        for dat in data:
            sens_val1.append(dat.getvalues()[0])
            sens_val2.append(dat.getvalues()[1])
            
            
timeStampfinal = [datetime.datetime.strptime(elem, '%Y-%m-%d %H:%M:%S.%f') for elem in time_stp]

print(len(sens_val),len(sens_val1),len(timeStampfinal))

# Plot the data
plt.figure(1)
plt.plot(timeStampfinal[:190],sens_val[:190], label='linear')

plt.figure(2)
plt.plot(timeStampfinal[:190],sens_val1[:190], label='linear')
plt.plot(timeStampfinal[:190],sens_val2[:190], label='linear')
# Add a legend
plt.legend()
# Show the plot
plt.show()


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




