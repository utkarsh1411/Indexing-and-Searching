#!flask/bin/python
from flask import Flask
from flask import request, jsonify
from flask_cors import CORS
import json
from datetime import datetime
from datetime import timedelta
from elasticsearch import Elasticsearch
from probables import (BloomFilter)
from probables import (HeavyHitters)

app = Flask(__name__)
CORS(app)
es = Elasticsearch()

#To manipulate date & time
def makeDate(d):
    now = datetime.utcnow()
    hours = (int(d[0:2])+4)%24
    now = now.replace(hour=hours)

@app.route('/')
def index():
    return "Hello, World!"

#code to get top k hitters
@app.route('/topK',methods = ['POST'])
def topKSearch():
   
    data = request.json
    startDate = datetime.utcnow() - timedelta(minutes=5)

    if data['from'] is not None:
        startDate = makeDate(data['from'])
     
    endDate = datetime.utcnow()
    if data['to'] is not None:
        endDate = makeDate(data['to'])
    k = int(data['k'])
    
    doc={
    "size":10000,
    "query":{
        "range":{
            "@timestamp":{
                "gte":startDate,
                "lt":endDate,
            }
        }
     },
    }
    #elastic searcg request
    res = es.search(index="twitterpost",  body=doc)
    
    data = (res['hits']['hits'])
    
    #Counting heavy hitters using Count Min Sketch
    hh =  HeavyHitters(num_hitters=k, width=1000, depth=5)
    for item in data:
        for word in item['_source']['tokenized']:
            hh.add(word)
    
    res = [];
    for item in hh.heavy_hitters:
        a = dict()
        
        a['word'] = item
        a['count'] = hh.heavy_hitters[item]
        res.append(a)
    
    return json.dumps(res)

   
# Calculate Term query
@app.route('/term',methods = ['POST'])
def termSearch():
   
    data = request.json
    startDate = datetime.utcnow() - timedelta(minutes=5)

    if data['from'] is not None:
        startDate = makeDate(data['from'])
     
    endDate = datetime.utcnow()
    if data['to'] is not None:
        endDate = makeDate(data['to'])
    tSearch = data['term']
    maxDoc = int(data['k'])
    
    doc={
    "size":maxDoc,
     "sort":[
         {"@timestamp":{"order":"desc"}},
     ],
     "query":{
         "bool":{
             "must":[
                 {
                "range":{
                    "@timestamp":{
                        "gte":startDate,
                        "lt":endDate,
                    }
               }
            },
             {
                 "term":{
                    "text":tSearch,
                   }
             }
            ]
         }
       }
    }
    
    #elastic search call
    res = es.search(index="twitterpost",  body=doc)
    
    data = (res['hits']['hits'])
    res = []
    for d in data:
        res.append(d['_source']['text'])
        
    return json.dumps(res)


#prefix method
@app.route('/prefix',methods = ['POST'])
def prefixSearch():
   
    data = request.json
    startDate = datetime.utcnow() - timedelta(minutes=5)

    if data['from'] is not None:
        startDate = makeDate(data['from'])
     
    endDate = datetime.utcnow()
    if data['to'] is not None:
        endDate = makeDate(data['to'])
    tSearch = data['term']
    maxDoc = int(data['k'])
    
    doc={
    "size":maxDoc,
     "sort":[
         {"@timestamp":{"order":"desc"}},
     ],
     "query":{
         "bool":{
             "must":[
                 {
                "range":{
                    "@timestamp":{
                        "gte":startDate,
                        "lt":endDate,
                    }
               }
            },
             {
                 "prefix":{
                    "text":tSearch,
                   }
             }
          ]
         }
     }
            
    }
    
    res = es.search(index="twitterpost",  body=doc)
    
    data = (res['hits']['hits'])
    res = []
    for d in data:
        res.append(d['_source']['text'])
    return json.dumps(res)

 
#Calculating sentiment
@app.route('/sentiment',methods = ['POST'])
def sentimentSearch():
   
    data = request.json
    startDate = datetime.utcnow() - timedelta(minutes=5)

    if data['from'] is not None:
        startDate = makeDate(data['from'])
     
    endDate = datetime.utcnow()
    
    if data['to'] is not None:
        endDate = makeDate(data['to'])
    
    
    doc={
     "size":100000,   
     "query":{
        "range":{
            "@timestamp":{
                "gte":startDate,
                "lt":endDate,
            }
        }
     },
      "aggregations": {
         "sentiment": {
            "terms": {
                "field": "sentiment"
            }
        },
       }
            
    }
    result = "positive"
    res = es.search(index="twitterpost",  body=doc)
    if(len(res['aggregations']['sentiment']['buckets'])>0):
        result = (res['aggregations']['sentiment']['buckets'][0]['key'])
    
    return result


#To calculate terms set 
@app.route('/terms',methods = ['POST'])
def termsSearch():
   
    data = request.json
    startDate = datetime.utcnow() - timedelta(minutes=5)

    if data['from'] is not None:
        startDate = makeDate(data['from'])
     
    endDate = datetime.utcnow()
    if data['to'] is not None:
        endDate = makeDate(data['to'])
    terms = data['terms']
    maxDoc = int(data['k'])
    minMatch = int(data['minMatch'])
    
    doc={
    "size": maxDoc,
    "sort":[
         {"@timestamp":{"order":"desc"}},
     ],
     "query":{
              "terms":{
                    "text":terms,
                     "minimum_should_match":minMatch
                   }
             }
     }
    
   
    res = es.search(index="twitterpost",  body=doc)
    
    data = (res['hits']['hits'])
    res = []
    for d in data:
        res.append(d['_source']['text'])
    return json.dumps(res)

 

if __name__ == '__main__':
    app.run(debug=True)

