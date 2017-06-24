
# coding: utf-8

# In[8]:

import boto3
from boto.s3.connection import S3Connection
import os
import json
import boto3
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
import glob
import logging
import logging.handlers
import time



#Loading 2 Json Config files

with open('config.json') as data_file:    
    data = json.load(data_file)
    
with open('initconfig.json') as data_file1:    
    data1 = json.load(data_file1)
    
# secret keys 

AWSAccess1=data["AWSAccess"]
AWSSecret1=data["AWSSecret"]

#json variables

# pprint(data)
link1=data["link"]
# print(link1)

state1=data["state"]
# print(state1)

stationId=data["StationId"]
    
linkpt1= data1["linkpart1"]
linkpt2= data1["linkpart2"]


#current date time
datestr = time.strftime("%d%m%Y")
datestr= datestr[0:4]+datestr[-2:]
# print (datestr)

#for log files datestring.
datestr2=time.strftime("%d%m%Y%H%M%S")
datestr2= datestr2[0:4]+datestr2[-8:-6]









#log generation of files on local directory 

LOG_FILENAME = datestr2+'.log'
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
fname = state1+"_"+datestr+stationId+".csv"

#Connection variables

c = boto.connect_s3(AWSAccess1, AWSSecret1)
conn = S3Connection(AWSAccess1, AWSSecret1)

#Create create bucket on S3 if doesnt exist
bucket = conn.create_bucket('team8njassignment1')
#print (bucket)
print(type(bucket))


#Fetch all the initial data and check if data exists already


bucket = c.get_bucket('team8njassignment1')
count =0
for key in bucket.list():
    lists3files=key.name.encode('utf-8')
    print (lists3files)
    count=count+1
print ('Number of files in S3 bucket ', count)


if count ==0:
    #init File merge 
    initfile1 = pd.read_csv(linkpt1)
    print("Shape of 1st file is :",initfile1.shape)
#     print (initfile1.head(n=1).iloc[:, [5]],)
#     print (initfile1.tail(n=1).iloc[:, [5]],)

    initfile2 = pd.read_csv(linkpt2)
    print("Shape of 1st file is :",initfile2.shape)
#     print (initfile2.head(n=1).iloc[:, [5]],)
#     print (initfile2.tail(n=1).iloc[:, [5]],)


    initfullmerge=pd.concat([initfile1,initfile2], axis=0).drop_duplicates().reset_index(drop=True)
    print("Shape of merged file is :",initfullmerge.shape)
    initfullmerge2=initfullmerge.drop_duplicates(['DATE'], keep='first')
    print("Shape of merged n duplicated removed file is :",initfullmerge2.shape)
#     print(initfullmerge2.head(5))
#     print (type(initfullmerge2))
    
#download on local directory init data    
    initialfilename='initfile.csv'
    initfullmerge2.to_csv(initialfilename,sep=',', index=False)
    # log file save event
    my_logger.info("A csv file named 'initfile' was saved in the local repository at:" +time.strftime("%d%m%Y%H%M%S"))

    
#congif.Json file update last init file.


    data["lastChangedFile"]= "initfile.csv"
    filename= 'config.json'
    with open('config.json', 'r') as f:
        data = json.load(f)
        data['lastChangedFile'] = initialfilename # <--- add `id` value.

    os.remove(filename)
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)
    #log json file change
    my_logger.info("JSON file, 'config.json' was updated at:" +time.strftime("%d%m%Y%H%M%S"))

    #......................Initial data created. = initfile.csv..................

# upload the current date data

b = c.get_bucket(bucket, validate=False)

k=Key(bucket)
k.key=fname
possiblekey=bucket.get_key("Rawdata/"+fname)
print(fname)
print('possible', possiblekey)
if possiblekey==None:
        url = link1
        r = requests.get(url)
        if r.status_code == 200:
        #Todays data extracted from link
            todaylink= pd.read_csv(link1)
            print("Shape of 1st file is :",todaylink.shape)
        todaylink= pd.read_csv(link1)
        #extracting 2nd(previous days) file into dataframe
        
        with open('config.json') as data_file:    
                data = json.load(data_file)   
                
        lastchangedfile= data["lastChangedFile"] 
        links3="https://s3.amazonaws.com/team8njassignment1/Rawdata/"+lastchangedfile
        print(lastchangedfile)
        
        r = requests.get(links3)
        if r.status_code == 200:
     #Todays data extracted from link
            prevdata= pd.read_csv(links3)
            print (prevdata.shape)
        prevdata= pd.read_csv(links3)
        print("Shape of second file is :",prevdata.shape)
#         prevdata= pd.read_csv(links3)   
        #Merge previous and today
        
        dailymerge=pd.concat([prevdata,todaylink], axis=0).drop_duplicates().reset_index(drop=True)
        print("Shape of merged file is :",dailymerge.shape)
        dailymerge2=dailymerge.drop_duplicates(['DATE'], keep='first')
        print("Shape of merged n duplicated removed file is :",dailymerge2.shape)
        
      
        #upload the file 
               
        k = Key(b)
        k.key = "Rawdata/"+fname
        k.content_type = r.headers['content-type']
        k.set_contents_from_string(r.content)
        print('successfully uploaded to s3')
        #log upload event
        my_logger.info("A file for the day was uploaded on S3 at:" +time.strftime("%d%m%Y%H%M%S"))

        
        #download current day file on local as well.
        
        dailymerge2.to_csv(fname,sep=',')
#         #log upload event
        my_logger.info("A file for the day was downloaded in the local repository at:" +time.strftime("%d%m%Y%H%M%S"))
        
        #congif.Json file daily update last changed file.

        print (fname)
        data["lastChangedFile"]= fname
        filename= 'config.json'
        with open('config.json', 'r') as f:
            data = json.load(f)
            data['lastChangedFile'] = fname # <--- add `id` value.

        os.remove(filename)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        #log json file update event 
        
        my_logger.info("JSON file 'config.json was updated with the new file name :" +time.strftime("%d%m%Y%H%M%S"))
        

else:
    #file already exist in s3 & log that event.
    print("the file already exists in s3")
    
    #log file already exists
    my_logger.info("An attempt was made to download an already existing file at:" +time.strftime("%d%m%Y%H%M%S"))




# In[ ]:



