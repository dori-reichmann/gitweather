import sys
import os
from google.cloud import bigquery
import json
import gzip
import datetime
import numpy

DATA_DIR = "./data/"

def query_commits(sample):
    if sample:        
        sql = open('commit_sample.sql').read().replace('\n',' ')    
    else:
        sql = open('commit.sql').read().replace('\n',' ')
    bigquery_client = bigquery.Client()
    it = bigquery_client.run_sync_query(sql)
    it.use_legacy_sql = False
    it.run()
    years = set()
    if it.complete:
        print("saving %d rows: "%it.total_rows,end="",flush=True)
        fname = os.path.normpath(os.path.join(DATA_DIR,'commits.json.gz'))
        if sample:
            fname = fname.replace('commits','commits_sample')
        with gzip.GzipFile(fname,"wb") as fout:
            for rec in it.rows:
                year = rec[0].year
                if year<1995 or year>2017:
                    continue
                years.add(year)
                frec = {"date" : rec[0].strftime('%Y-%m-%d'), "count" : rec[1]}
                fout.write((json.dumps(frec)+"\n").encode('utf8'))
        print("saved ",fname)            
    return years

def query_weather(year):
    bigquery_client = bigquery.Client()
    sql = open("weather.sql").read().replace('\n',' ')
    sql = sql.replace('YYYYY',str(year))
    it = bigquery_client.run_sync_query(sql)
    it.use_legacy_sql = False
    it.run()
    if it.complete:
        print("saving %d rows: "%it.total_rows,end="",flush=True)
        fname = os.path.normpath(os.path.join(DATA_DIR,'weather_%d.json.gz'%year))
        with gzip.GzipFile(fname,"wb") as fout:
            for rec in it.rows:
                frec = {"date" : rec[0].strftime('%Y-%m-%d'),
                        "element" : rec[1],
                        "avg" : rec[2],"max" : rec[3], "min": rec[4]}
                fout.write((json.dumps(frec)+"\n").encode('utf8'))
        print("saved ",fname)
        return True
    else:
        print("failed year = %d"%year)
        return False

def query_data(sample=True):
    os.system('mkdir -p %s'%DATA_DIR)
    years = query_commits(sample)
    missing = []
    hql = open("weather.sql").read().replace('\n',' ')
    for year in years:
        success  =query_weather(year)
        if not(success):
            missing.append(year)
    print("")
    return missing

def load_data(fname):
    file_iter = lambda f: (json.loads(x.decode('utf-8').strip()) for x in gzip.GzipFile(f))
    print("loading ",fname)
    labels = numpy.array(list((datetime.datetime.strptime(x['date'],'%Y-%m-%d').toordinal(),
                               int(x['count'])) for x in file_iter(fname)),dtype=numpy.int32)
    years = set()
    features = list(numpy.zeros((labels.shape[0],1000),dtype=numpy.float32) for i in range(3))
    date2index = dict()
    key2index = dict()
    for i in range(labels.shape[0]):
        date2index[labels[i,0]] = i
        years.add(datetime.datetime.fromordinal(labels[i,0]).year)
    for year in sorted(years):
        f = os.path.join(os.path.split(fname)[0],"weather_%d.json.gz"%year)
        print("\rloading ",f,end="",flush=True)
        for rec in file_iter(f):
            d = datetime.datetime.strptime(rec['date'],'%Y-%m-%d')
            i = date2index.get(d.toordinal())
            if i is None:
                continue
            j = key2index.setdefault(rec.get('element'),len(key2index))
            features[0][i,j] = rec['min']
            features[1][i,j] = rec['avg']
            features[2][i,j] = rec['max']
    print("")
    for i in range(3):
        features[i] = features[i][:,:len(key2index)]        
    features = numpy.hstack(features)
    features_date = numpy.zeros((labels.shape[0],27),dtype = numpy.float32)

    fun = lambda d: datetime.datetime.fromordinal(d).year
    features_date[:,0] = list(map(fun,labels[:,0]))
    fun = lambda d: datetime.datetime.fromordinal(d).weekday()
    features_date[:,1] = list(map(fun,labels[:,0]))
    fun = lambda d: datetime.datetime.fromordinal(d).timetuple().tm_yday
    features_date[:,2] = list(map(fun,labels[:,0]))
    for j in range(12):
        fun = lambda d: numpy.cos((j+1.0)*2*numpy.pi*datetime.datetime.fromordinal(d).timetuple().tm_yday/365.0)
        features_date[:,3+2*j] = list(map(fun,labels[:,0]))
        fun = lambda d: numpy.sin((j+1.0)*2*numpy.pi*datetime.datetime.fromordinal(d).timetuple().tm_yday/365.0)
        features_date[:,3+2*j+1] = list(map(fun,labels[:,0]))
    return labels,features,features_date,key2index