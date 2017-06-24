
# coding: utf-8

# In[5]:

import boto3
from boto.s3.connection import S3Connection
import os
import json
import boto.s3
import sys
import datetime
from boto.s3.key import Key
from pprint import pprint
import pandas as pd
import urllib
import csv
import io
import requests
import time
import json
import datetime
from pprint import pprint
import scipy
import numpy as np
import glob
import logging
import logging.handlers


# Loading the config.json file and get merged Csv

with open('configWrangle.json') as data_file:    
        data = json.load(data_file)   

#Extracting Data From last File Created.

rawdatafile= data["rawData"]   
print(rawdatafile)

#Read from S3

        #data to be extracted from link
rawdata1 = pd.read_csv(rawdatafile)
    
        


rawdata=rawdata1
print("Shape of second file is :",rawdata.shape)
# print(rawdata.head(5))
print (rawdata.dtypes)
print (rawdata1.head(3))
print (rawdata1.dtypes)


# secret keys 
AWSAccess1=data["AWSAccess"]
AWSSecret1=data["AWSSecret"]

#state

state1=data["state"]
# print(state1)

stationId=data["StationId"]



#current date time
datestr = time.strftime("%d%m%Y")
datestr= datestr[0:4]+datestr[-2:]
# print (datestr)

#for log files datestring.
datestr2=time.strftime("%d%m%Y%H%M%S")
datestr2= datestr2[0:4]+datestr2[-8:-6]


#log generation of files on local directory 

LOG_FILENAME = datestr2+'_clean.log'
# Set up a specific logger with our desired output level
my_logger = logging.getLogger('MyLogger')

if not my_logger.handlers:

    my_logger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
    handler = logging.handlers.TimedRotatingFileHandler( filename= LOG_FILENAME, when= 'd', interval= 1,
                                                    backupCount= 120)

    my_logger.addHandler(handler)
 # create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%m/%d/%Y %I:%M:%S %p')

# add formatter to handler
handler.setFormatter(formatter)



# See what files are created
logfiles = glob.glob('%s*' % LOG_FILENAME)

for filename in logfiles:
    print (filename)


#.............................................Log file generated..........................................................

        

    
#fname for current date files 
fname = state1+"_"+datestr+stationId+"_clean.csv"

#Connection variables

c = boto.connect_s3(AWSAccess1, AWSSecret1)
conn = S3Connection(AWSAccess1, AWSSecret1)    
bucket = c.get_bucket('team8njassignment1')
b = c.get_bucket(bucket, validate=False)



# #Create bucket on S3 if doesnt exist
# bucket = conn.create_bucket('team8njassignment1')
# #print (bucket)
# print(type(bucket))
  
    
    
b = c.get_bucket(bucket, validate=False)

k=Key(bucket)
k.key=fname
possiblekey=bucket.get_key("Rawdata/"+fname)
print(fname)
print('possible', possiblekey)
fileexists=0
if possiblekey==None:
    
#     r = requests.get(url)
#     if r.status_code == 200:
        #Todays data extracted from link
#         todaylink= pd.read_csv(link1)
#         print("Shape of 1st file is :",todaylink.shape)
        fileexists=1
        print("File Exists")

#Extracting Data From last File Created.

rawdatafile= data["rawData"]   
print(rawdatafile)

#Read from S3

r = requests.get(rawdatafile)
if r.status_code == 200:
        #data to be extracted from link
         rawdata1 = pd.read_csv(rawdatafile)
    
        


rawdata=rawdata1
print("Shape of second file is :",rawdata.shape)
# print(rawdata.head(5))
print (rawdata.dtypes)
print (rawdata1.head(3))
print (rawdata1.dtypes)


#converting all int and float rows to Numeric datatype
rawdata.apply(pd.to_numeric, errors='ignore')


# Calculating the threshold value of for the maximum number of NaN values that can be present in the column
rowCount=len(rawdata.index)
rowCountpercent=rowCount*5/100
print (rowCountpercent)
treshold=rowCount=rowCountpercent
print (treshold)

# deleting the columns which exceed the threshold value of NaN present in a column i.e.95%
rawdata=rawdata.dropna(thresh=len(rawdata) - treshold, axis=1)
print (rawdata.shape)
print (rawdata.dtypes)

replacezerorawdata=rawdata.replace('NaN',0)
print(replacezerorawdata.head(5))
datasummary=(rawdata == 0).sum(axis=0)
print (datasummary)


rawdata = replacezerorawdata.loc[:, (replacezerorawdata != 0).any(axis=0)]
print (rawdata.shape)
print (rawdata.head(5))

rawdata.dropna(thresh=len(rawdata) - treshold, axis=1)

print (rawdata.shape)
rawdata = rawdata[rawdata.REPORTTPYE != 'SOD']
print (rawdata.shape)

print (rawdata.dtypes)
rawdata.head(5)

datasummary=(rawdata == 0).sum(axis=0)
print ("Number of zeroes in a column",datasummary)

time_func = lambda x: pd.Timestamp(pd.to_datetime(x, format = '%H%M'))

dataforsunrise=rawdata['DAILYSunrise'].apply(time_func)
dataforsunset=rawdata['DAILYSunset'].apply(time_func)
daylenght=(dataforsunset-dataforsunrise).astype('timedelta64[m]')/60

print(dataforsunset.head(3))
print(dataforsunrise.head(3))
print(daylenght.head(3))

# daylen1= (daylenght.groupby('year', 'month')['hour']).mean().reset_index()
# print(daylen1)

rawdata['LENGTHOFDAY']=daylenght.abs()
print (rawdata.head(5))


if   fileexists==0:

    k = Key(b)
    k.key = "CleanData/"+fname
    k.content_type = r.headers['content-type']
    k.set_contents_from_string(r.content)
# url = k.generate_url(expires_in=0, query_auth=False)
# print (url)



    print('successfully uploaded to s3')
#update json 

 #log upload event
    my_logger.info("A clean file for the day was uploaded on S3 at:" +time.strftime("%d%m%Y%H%M%S"))



    cleanfilelink="https://s3.amazonaws.com/team8njassignment1/CleanData/"+fname+"_clean.csv"

#congif.Json file daily update last changed file.
# print (fname)
    data["cleanData"]= cleanfilelink
    filename= 'configWrangle.json'
    with open('configWrangle.json', 'r') as f:
        data = json.load(f)
    data['cleanData'] = cleanfilelink # <--- add `id` value.

    os.remove(filename)
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)
        #log json file update event 

    
    my_logger.info("JSON file 'config.json was updated with the new file name :" +time.strftime("%d%m%Y%H%M%S"))
else:
    my_logger.info("An attempt was made to clean an already cleaned data at: " +time.strftime("%d%m%Y%H%M%S"))
    print ("File Exists ")


# In[ ]:




# In[ ]:



