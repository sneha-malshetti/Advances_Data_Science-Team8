
# coding: utf-8

# In[1]:

import os

from flask import Flask
from flask import request, render_template,url_for, request, session, redirect
from flask_pymongo import PyMongo
from pymongo import MongoClient
import pandas as pd
import numpy as np

# In[2]:


# client3.zillowtestdb.authenticate("admin","admin")
# client3 = MongoClient("mongodb://52.87.172.158")

client3 = MongoClient("mongodb://54.166.125.102")
print(client3)
db = client3['zillowdb']






app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'zillowdb'
app.config['MONGO_URI'] = 'mongodb://54.166.125.102:27017/zillowdb'
app.config['MONGO3_HOST'] = 'ec2-54-166-125-102.compute-1.amazonaws.com'
app.config['MONGO3_PORT'] = 27017

mongo = PyMongo(app) 
# client = MongoClient("mongodb://52.87.172.158")
# print(client)

#connect to default db

# records=db.zillowdata.find().limit(10)
# for rows in records:
#     print (rows)

db3= client3["zillowtestdb"] 
@app.route('/')
def home():
    print ("index")
    return render_template('index.html')
                


@app.route('/api/visitors', methods=['POST'])
def put_visitor():

    listofmongodb = pd.DataFrame(list(db.zillowdata.find().limit(5)))
    print(listofmongodb)
    b=listofmongodb
    for rows in listofmongodb:
        print (rows)
    lat = float(request.form['latitude'])
    lon = float(request.form['longitude'])
    print (lat,",",lon)

#     print (c)
#     collection=db.zillowdata.find_one({'latitude': c})
#     print ("collection ")
# #     collection.find( { latitude: "A" }, { parcelid: 1 } )
#
#     b =collection['parcelid']
#     print (b)
#     print (collection)
    return render_template('index.html', plat=lat, plon=lon)




if __name__ == '__main__':

    port = int(os.environ.get('PORT', 5000))
#     app.run(host='0.0.0.0', port=port, debug=true)
    app.run()        
            

            





