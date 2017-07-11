
# coding: utf-8

# In[9]:

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


# In[10]:

rawdataspecificrows= pd.read_csv("properties_2016.csv")
        
print(rawdataspecificrows.head(5))


# # Merging columns 

# In[11]:

train_df = pd.read_csv("train_2016_v2.csv", parse_dates=["transactiondate"])
train_df.shape     

# merge train data with properties CSV
train_df =pd.merge(train_df, rawdataspecificrows, on ='parcelid', how = 'outer')
train_df.shape


# train_df.to_csv('mergeddata.csv', index = False)

print ("successfully saved as a CSV file")


# In[1]:

rawdata=train_df[["parcelid","airconditioningtypeid","architecturalstyletypeid","basementsqft",
                 "bedroomcnt","buildingclasstypeid","decktypeid",
                 "finishedsquarefeet6", "typeconstructiontypeid", "finishedsquarefeet12","finishedsquarefeet13","finishedsquarefeet15",
                 "finishedsquarefeet50", "finishedfloor1squarefeet","calculatedfinishedsquarefeet",
                 "fips","fireplacecnt","fullbathcnt","garagecarcnt","garagetotalsqft","hashottuborspa",
                 "heatingorsystemtypeid","latitude","longitude","lotsizesquarefeet","poolcnt", "poolsizesum",
                 "propertycountylandusecode","propertylandusetypeid", "unitcnt",
                 "propertyzoningdesc","rawcensustractandblock","regionidcity","regionidcounty",
                 "regionidzip","roomcnt","threequarterbathnbr",
                 "yardbuildingsqft17","yardbuildingsqft26","yearbuilt",
                 "numberofstories","structuretaxvaluedollarcnt",
                 "assessmentyear","landtaxvaluedollarcnt","taxamount","taxdelinquencyyear", "transactiondate","logerror"]]

rawdata.head(5)


# In[13]:

print(rawdata[["finishedsquarefeet6", "finishedsquarefeet12","finishedsquarefeet13","finishedsquarefeet15",
                 "finishedsquarefeet50", "finishedfloor1squarefeet","calculatedfinishedsquarefeet"]])

count = rawdata[["finishedsquarefeet6", "finishedsquarefeet12","finishedsquarefeet13","finishedsquarefeet15",
                 "finishedsquarefeet50", "finishedfloor1squarefeet","calculatedfinishedsquarefeet"]].count(axis=0)
print ("Count:",count)


# rawdata.finishedsquarefeet50.fillna(rawdata.finishedfloor1squarefeet, inplace=True)
# del df['finishedfloor1squarefeet']

# rawdata[["finishedsquarefeet6", "finishedsquarefeet12","finishedsquarefeet13","finishedsquarefeet15",
#                  "finishedsquarefeet50", "finishedfloor1squarefeet","calculatedfinishedsquarefeet"]]


# In[14]:

rawdata.finishedsquarefeet50.fillna(rawdata.finishedfloor1squarefeet, inplace=True)


rawdata[["finishedsquarefeet6", "finishedsquarefeet12","finishedsquarefeet13","finishedsquarefeet15",
                 "finishedsquarefeet50", "finishedfloor1squarefeet","calculatedfinishedsquarefeet"]]

count = rawdata[["finishedsquarefeet6", "finishedsquarefeet12","finishedsquarefeet13","finishedsquarefeet15",
                 "finishedsquarefeet50", "finishedfloor1squarefeet","calculatedfinishedsquarefeet"]].count(axis=0)
print ("Count after merging finishedsquarefeet50 and finishedfloor1squarefeet :",count)
# print ("The number of values in the column are : ",)


# In[15]:

rawdata.calculatedfinishedsquarefeet.fillna(rawdata.finishedsquarefeet12, inplace=True)
rawdata.calculatedfinishedsquarefeet.fillna(rawdata.finishedsquarefeet13, inplace=True)
rawdata.calculatedfinishedsquarefeet.fillna(rawdata.finishedsquarefeet15, inplace=True)
rawdata.calculatedfinishedsquarefeet.fillna(rawdata.finishedsquarefeet6, inplace=True)


rawdata[["finishedsquarefeet6", "finishedsquarefeet12","finishedsquarefeet13","finishedsquarefeet15",
                 "finishedsquarefeet50", "finishedfloor1squarefeet","calculatedfinishedsquarefeet"]]

count = rawdata[["finishedsquarefeet6", "finishedsquarefeet12","finishedsquarefeet13","finishedsquarefeet15",
                 "finishedsquarefeet50", "finishedfloor1squarefeet","calculatedfinishedsquarefeet"]].count(axis=0)
print ("Count after merging finishedsquarefeet50 and finishedfloor1squarefeet :",count)


# In[16]:

# replacing airconditioningtypeid to 5 default type for none
rawdata.airconditioningtypeid.fillna(5, inplace=True)
rawdata.airconditioningtypeid=rawdata.airconditioningtypeid.astype(int)
rawdata.head(5)


# In[17]:

# replacing architecturalstyletypeid to 19 default type for other
rawdata.architecturalstyletypeid.fillna(19, inplace=True)
rawdata.architecturalstyletypeid=rawdata.architecturalstyletypeid.astype(int)
rawdata.head(5)


# In[18]:

# replacing basementsqft to 0 default square feet 
rawdata.basementsqft.fillna(0, inplace=True)
rawdata.basementsqft=rawdata.basementsqft.astype(int)
rawdata.head(5)


# In[19]:

# replacing basementsqft to 0 default square feet 
rawdata.bedroomcnt.fillna(0, inplace=True)
rawdata.bedroomcnt=rawdata.bedroomcnt.astype(int)
rawdata.bedroomcnt.head(5)


# In[20]:

# replacing buildingclasstypeid to 5 default other  
rawdata.buildingclasstypeid.fillna(5, inplace=True)
rawdata.buildingclasstypeid=rawdata.buildingclasstypeid.astype(int)
rawdata.buildingclasstypeid.head(5)


# In[21]:

#listing unique values in col
rawdata.decktypeid.unique()


# In[22]:

# rawdata[['decktypeid']].notnull()


# In[23]:

rawdata.groupby('decktypeid')['parcelid'].count()


# In[24]:

# rawdata['decktypeid'].replace(66, 'True')
# rawdata.decktypeid.fillna('False', inplace=True)
# rawdata.decktypeid.head(5)

rawdata['decktypeid']=rawdata['decktypeid'].notnull()

rawdata['decktypeid'].head(5)


# In[25]:

rawdata.groupby('decktypeid')['parcelid'].count()


# In[26]:

# check which has the higher value
print ("finishedsquarefeet50 sum = ",rawdata["finishedsquarefeet50"].sum())

print ("finishedfloor1squarefeet sum = ",rawdata["finishedfloor1squarefeet"].sum())


# In[27]:

rawdata.shape


# In[28]:

# dropping column finishedfloor1squarefeet 

del rawdata['finishedfloor1squarefeet']

rawdata.shape


# In[29]:

rawdata.finishedsquarefeet50.fillna(0, inplace=True)
rawdata.finishedsquarefeet50=rawdata.finishedsquarefeet50.astype(int)
rawdata["finishedsquarefeet50"].head(5)
rawdata.groupby('finishedsquarefeet50')['parcelid'].count()




# print ("calculatedfinishedsquarefeet sum = ",rawdata["calculatedfinishedsquarefeet"].sum())

# print ("finishedsquarefeet12 sum = ",rawdata["finishedsquarefeet12"].sum())

# print ("finishedsquarefeet13 sum = ",rawdata["finishedsquarefeet13"].sum())

# print ("finishedsquarefeet15 sum = ",rawdata["finishedsquarefeet15"].sum())

# print ("finishedsquarefeet6 sum = ",rawdata["finishedsquarefeet6"].sum())


# In[30]:

rawdata.calculatedfinishedsquarefeet.fillna(0, inplace=True)
rawdata.calculatedfinishedsquarefeet=rawdata.calculatedfinishedsquarefeet.astype(int)
rawdata["calculatedfinishedsquarefeet"].head(5)
rawdata.groupby('calculatedfinishedsquarefeet')['parcelid'].count()


# In[31]:

rawdata.groupby('fips')['parcelid'].count()


# In[32]:

rawdata.fips.fillna(0, inplace=True)
rawdata.fips=rawdata.fips.astype(int)
rawdata["fips"].head(5)
# print (rawdata.groupby('fips')['parcelid'].count())


# In[33]:

rawdata.groupby('fips')['parcelid'].count()


# In[34]:

rawdata.fireplacecnt.fillna(0, inplace=True)
rawdata.fireplacecnt=rawdata.fireplacecnt.astype(int)
rawdata["fireplacecnt"].head(5)
rawdata.groupby('fireplacecnt')['parcelid'].count()


# In[35]:


rawdata.fullbathcnt.fillna(0, inplace=True)
rawdata.fullbathcnt=rawdata.fullbathcnt.astype(int)
rawdata["fullbathcnt"].head(5)
rawdata.groupby('fullbathcnt')['parcelid'].count()


# In[36]:


rawdata.threequarterbathnbr.fillna(0, inplace=True)
rawdata.threequarterbathnbr=rawdata.threequarterbathnbr.astype(int)
rawdata["threequarterbathnbr"].head(5)
rawdata.groupby('threequarterbathnbr')['parcelid'].count()


# In[37]:


rawdata.garagecarcnt.fillna(0, inplace=True)
rawdata.garagecarcnt=rawdata.garagecarcnt.astype(int)
rawdata["garagecarcnt"].head(5)
rawdata.groupby('garagecarcnt')['parcelid'].count()


# In[38]:


rawdata.garagetotalsqft.fillna(0, inplace=True)
rawdata["garagetotalsqft"].head(5)


# In[39]:


rawdata.hashottuborspa.fillna("False", inplace=True)
rawdata["hashottuborspa"].head(5)
rawdata.groupby('hashottuborspa')['parcelid'].count()


# In[40]:


rawdata.heatingorsystemtypeid.fillna(13, inplace=True)
rawdata.heatingorsystemtypeid=rawdata.heatingorsystemtypeid.astype(int)
rawdata["heatingorsystemtypeid"].head(5)
rawdata.groupby('heatingorsystemtypeid')['parcelid'].count()


# In[41]:



rawdata['poolcnt']=rawdata['poolcnt'].notnull()

rawdata['poolcnt'].head(5)

rawdata.groupby('poolcnt')['parcelid'].count()


# In[42]:


rawdata.propertylandusetypeid.fillna(291, inplace=True)
rawdata.propertylandusetypeid=rawdata.propertylandusetypeid.astype(int)
rawdata["propertylandusetypeid"].head(5)
rawdata.groupby('propertylandusetypeid')['parcelid'].count()


# In[43]:

rawdata.regionidcounty.fillna(0, inplace=True)
rawdata.regionidcounty=rawdata.regionidcounty.astype(int)
rawdata["regionidcounty"].head(5)
rawdata.groupby('regionidcounty')['parcelid'].count()


# In[44]:

rawdata.regionidcity.fillna(0, inplace=True)
rawdata.regionidcity=rawdata.regionidcity.astype(int)
rawdata["regionidcity"].head(5)
rawdata.groupby('regionidcity')['parcelid'].count()


# In[45]:

# regionidzip

rawdata.regionidzip.fillna(0, inplace=True)
rawdata.regionidzip=rawdata.regionidzip.astype(int)
rawdata["regionidzip"].head(5)
rawdata.groupby('regionidzip')['parcelid'].count()


# In[46]:

# roomcnt

## sum of rooms (((((check)))))

rawdata.roomcnt.fillna(0, inplace=True)
rawdata.roomcnt=rawdata.roomcnt.astype(int)
rawdata["roomcnt"].head(5)
rawdata.groupby('roomcnt')['parcelid'].count()



# In[47]:

# # typeconstructiontypeid
rawdata.typeconstructiontypeid.fillna(14, inplace=True)
rawdata.typeconstructiontypeid=rawdata.typeconstructiontypeid.astype(int)
rawdata["typeconstructiontypeid"].head(5)
rawdata.groupby('typeconstructiontypeid')['parcelid'].count()


# In[48]:

# unitcnt
rawdata.unitcnt.fillna(0, inplace=True)
rawdata.unitcnt=rawdata.unitcnt.astype(int)
# rawdata["unitcnt"].head(5)
rawdata.groupby('unitcnt')['parcelid'].count()



# In[49]:

# yardbuildingsqft17

rawdata.yardbuildingsqft17.fillna(0, inplace=True)
rawdata.yardbuildingsqft17=rawdata.yardbuildingsqft17.astype(int)
rawdata["yardbuildingsqft17"].head(5)
# rawdata.groupby('yardbuildingsqft17')['parcelid'].count()


# In[50]:

# yardbuildingsqft26

rawdata.yardbuildingsqft26.fillna(0, inplace=True)
rawdata.yardbuildingsqft26=rawdata.yardbuildingsqft26.astype(int)
rawdata["yardbuildingsqft26"].head(5)
rawdata.groupby('yardbuildingsqft26')['parcelid'].count()


# In[51]:

# yearbuilt

rawdata.yearbuilt.fillna(0, inplace=True)
rawdata.yearbuilt=rawdata.yearbuilt.astype(int)
rawdata["yearbuilt"].head(5)
rawdata.groupby('yearbuilt')['parcelid'].count()


# In[52]:

# numberofstories

rawdata.numberofstories.fillna(0, inplace=True)
rawdata.numberofstories=rawdata.numberofstories.astype(int)
rawdata["numberofstories"].head(5)
rawdata.groupby('numberofstories')['parcelid'].count()


# In[53]:

# structuretaxvaluedollarcnt

rawdata.structuretaxvaluedollarcnt.fillna(0, inplace=True)
rawdata.structuretaxvaluedollarcnt=rawdata.structuretaxvaluedollarcnt.astype(int)
rawdata["structuretaxvaluedollarcnt"].head(5)
rawdata.groupby('structuretaxvaluedollarcnt')['parcelid'].count()


# In[54]:

# landtaxvaluedollarcnt

rawdata.landtaxvaluedollarcnt.fillna(0, inplace=True)
rawdata.landtaxvaluedollarcnt=rawdata.landtaxvaluedollarcnt.astype(int)
rawdata["landtaxvaluedollarcnt"].head(5)
# rawdata.groupby('landtaxvaluedollarcnt')['parcelid'].count()


# In[55]:

# taxamount


rawdata.taxamount.fillna(0, inplace=True)
rawdata.taxamount=rawdata.taxamount.astype(int)
rawdata["taxamount"].head(5)
rawdata.groupby('taxamount')['parcelid'].count()


# In[56]:

# poolsizesum


rawdata.poolsizesum.fillna(0, inplace=True)
rawdata.poolsizesum=rawdata.poolsizesum.astype(int)
# rawdata["poolsizesum"].head(5)
rawdata.groupby('poolsizesum')['parcelid'].count()


# In[57]:

# taxdelinquencyyear

rawdata.taxdelinquencyyear.fillna(0, inplace=True)
rawdata.taxdelinquencyyear=rawdata.taxdelinquencyyear.astype(int)

rawdata["taxdelinquencyyear"].head(5)
rawdata.groupby('taxdelinquencyyear')['parcelid'].count()

rawdata.rename(index=str, columns={"taxdelinquencyyear": "taxduetill2015", "poolcnt": "poolpresent"})


# In[58]:

# # To rename multiple columns 
# df.rename(index=str, columns={"A": "a", "C": "c"})
# # to fill values of one column from another and deleting the other column 
# df.Temp_Rating.fillna(df.Farheit, inplace=True)
# del df['Farheit']
# # to count the nnumber on not null values in a column 
# df[['a', 'b', 'c', 'd', 'e']].apply(lambda x: sum(x.notnull()), axis=1)


# In[59]:

rawdata.shape 


# In[60]:

# del rawdata['finishedfloor1squarefeet']
del rawdata['finishedsquarefeet6']
del rawdata['finishedsquarefeet12']
del rawdata[ 'finishedsquarefeet13']
del rawdata['finishedsquarefeet15']
print (rawdata.shape)
print (rawdata.dtypes)


# In[61]:

rawdata.lotsizesquarefeet.fillna(0, inplace=True)
# rawdata.lotsizesquarefeet=rawdata.lotsizesquarefeet
# rawdataloc["lotsizesquarefeet"].head(5)
print(rawdata.groupby('lotsizesquarefeet')['parcelid'].count())


# In[67]:


# rawdata.latitude.fillna(0, inplace=True)
# rawdata.latitude=rawdata.latitude
# # rawdataloc["latitude"].head(5)
# print(rawdata.groupby('latitude')['parcelid'].count())


# rawdata.longitude.fillna(0, inplace=True)
# # rawdata["longitude"].head(5)
# # rawdata['count']=rawdata.groupby('longitude')['parcelid'].count()
# rawdata.groupby('count')['longitude'].count()



# rawdata = rawdata[np.isfinite(rawdata['EPS'])]
print (rawdata.shape)


rawdata = rawdata[np.isfinite(rawdata['latitude'])]
rawdata = rawdata[np.isfinite(rawdata['longitude'])]
print (rawdata.shape)


# In[68]:

rawdata.loc[:, rawdata.isnull().any()]


# In[69]:

# rawdataloc=rawdata[["latitude", "longitude", "parcelid"]]


# In[70]:

rawdata.shape


# In[71]:

# rawdata.latitude=rawdata.latitude.astype('S32')
# rawdata.longitude=rawdata.longitude.astype('S32')
# rawdata.propertycountylandusecode=rawdata.propertycountylandusecode.astype('S32')
# rawdata.propertyzoningdesc=rawdata.propertyzoningdesc.astype('S32')
# rawdata.rawcensustractandblock=rawdata.rawcensustractandblock.astype('S32')
# rawdata.assessmentyear=rawdata.assessmentyear.astype('S32')


# rowsfilteredrawdata = rawdata[((rawdata.parcelid!= 0)) |  ((rawdata.airconditioningtypeid!= 5) & (rawdata.architecturalstyletypeid!= 19) & (rawdata.basementsqft!=0) & 
#                  (rawdata.bedroomcnt!= 0) & (rawdata.buildingclasstypeid!= 5) & 
#                  (rawdata.finishedsquarefeet50!= 0) & (rawdata.calculatedfinishedsquarefeet!=0) & 
#                  (rawdata.fips!= 0) & (rawdata.fireplacecnt!= 0) & (rawdata.fullbathcnt!= 0) & (rawdata.garagecarcnt!= 0) & (rawdata.garagetotalsqft!= 0)  & 
#                  (rawdata.heatingorsystemtypeid!= 13)  & (rawdata.lotsizesquarefeet!= 0) & (rawdata.poolcnt!= 0)  & (rawdata.unitcnt!=0) & 
#                  (rawdata.propertyzoningdesc!= 0) &  (rawdata.regionidcity!= 0) & (rawdata.regionidcounty!=0) & 
#                  (rawdata.regionidzip!= 0) & (rawdata.roomcnt!= 0) & (rawdata.threequarterbathnbr!=0) & 
#                  (rawdata.yardbuildingsqft17!= 0) & (rawdata.yardbuildingsqft26!= 0) & (rawdata.yearbuilt!=0) & 
#                  (rawdata.numberofstories!= 0) & (rawdata.structuretaxvaluedollarcnt!=0) & 
#                  (rawdata.landtaxvaluedollarcnt!= 0) & (rawdata.taxamount!= 0) & (rawdata.taxdelinquencyyear != 0))]



# print (rowsfilteredrawdata.dtypes           )


# In[72]:

rawdata.shape


# In[ ]:




# In[22]:

import boto3
from boto.s3.connection import S3Connection
import os
import json
import boto3
import boto.s3
#Loading 2 Json Config files

with open('config.json') as data_file:    
    data = json.load(data_file)
 
# secret keys 

AWSAccess1=data["AWSAccess"]
AWSSecret1=data["AWSSecret"]

#Connection variables




c = boto.connect_s3(AWSAccess1, AWSSecret1)
conn = S3Connection(AWSAccess1, AWSSecret1)    

bucket = c.get_bucket('team8njassignment2')
b = c.get_bucket(bucket, validate=False)
fname="wrangleddata.csv"
print(type(bucket))


# upload the current date data

k=Key(bucket)
k.key=fname

possiblekey=bucket.get_key(fname)


count =0
for key in bucket.list():
    lists3files=key.name.encode('utf-8')
    print (lists3files)
    count=count+1
    print ('Number of files in S3 bucket ', count)


    k = Key(b)
    k.Key = fname
    k.content_type = r.headers['content-type']
    k.set_contents_from_string(r.content)
    print('successfully uploaded to s3')



if   fileexists==0:

    k = Key(b)
    k.key = "wrangleddata.csv"
    k.content_type = r.headers['content-type']
    k.set_contents_from_string(r.content)


# In[74]:

rawdata.to_csv('wrangleddata.csv', index=False)

print ("successfully saved as a CSV file")


# In[101]:

# df = df[df.line_race != 0]
# df1 = df[(df.a != -1) & (df.b != -1)
# df2 = df[(df.a != -1) | (df.b != -1)]


# In[ ]:



