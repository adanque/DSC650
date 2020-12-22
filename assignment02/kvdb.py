"""
Author:     Alan Danque
Date:       20201203
Class:      DSC 650
Exercise:   2.1
"""
import ujson
import json
from pathlib import Path
import os
import pickle
import pandas as pd
#import s3fs

#def read_cluster_csv(file_path, endpoint_url='https://storage.budsc.midwest-datascience.com'):
def read_cluster_csv(file_path):
    df = pd.read_csv(file_path)
    """
    :param file_path: 
    :param endpoint_url: 
    :return: 

    s3 = s3fs.S3FileSystem(
        anon=True,
        client_kwargs={
            'endpoint_url': endpoint_url
        }
    )
    """
    return df
    #return pd.read_csv(s3.open(file_path, mode='rb'))
    #return pd.read_csv("C:/Users/aland/class/DSC650/dsc650/data/external/tidynomicon/site.csv", mode='rb')

current_dir = Path(os.getcwd()).absolute()
results_dir = current_dir.joinpath('results')
kv_data_dir = results_dir.joinpath('kvdb')
kv_data_dir.mkdir(parents=True, exist_ok=True)

people_json = kv_data_dir.joinpath('people.pickle')
visited_json = kv_data_dir.joinpath('visited.pickle')
sites_json = kv_data_dir.joinpath('sites.pickle')
measurements_json = kv_data_dir.joinpath('measurements.pickle')

class KVDB(object):
    def __init__(self, db_path):
        self._db_path = Path(db_path)
        self._db = {}
        self._load_db()

    def _load_db(self):
        if self._db_path.exists():
            #with open(self._db_path) as f:
            #    self._db = json.load(f)
            with open(self._db_path, 'rb') as f:
                self._db =pickle.load(f)

    def get_value(self, key):
        return self._db.get(key)

    def set_value(self, key, value):
        self._db[key] = value

    def save(self):
        #with open(self._db_path, 'w') as f:
        with open(self._db_path, 'wb') as f:
            pickle.dump(self._db, f, protocol=pickle.HIGHEST_PROTOCOL)
            #json.dump(self._db, f, indent=2)

    def saveu(self):
        #with open(self._db_path, 'w') as f:
        with open(self._db_path, 'wb') as f:
            pickle.dump(self._db, f, protocol=pickle.HIGHEST_PROTOCOL)
            #f.write(ujson.dumps(self._db))

    def load(self):
        # Load data (deserialize)
        with open(self._db_path, 'rb') as handle:
            unserialized_data = pickle.load(handle)
        print(your_data == unserialized_data)

def create_sites_kvdb():
    db = KVDB(sites_json)
    df = read_cluster_csv('c:/Users/aland/class/DSC650/dsc650/data/external/tidynomicon/site.csv')
    for site_id, group_df in df.groupby('site_id'):
        db.set_value(site_id, group_df.to_dict(orient='records')[0])
        print(db.get_value(site_id))
    db.save()

def create_people_kvdb():
    db = KVDB(people_json)
    df = read_cluster_csv('c:/Users/aland/class/DSC650/dsc650/data/external/tidynomicon/person.csv')
    for person_id, group_df in df.groupby('person_id'):
        db.set_value(person_id, group_df.to_dict(orient='records')[0])
        print(db.get_value(person_id))
    db.save()

def create_visits_kvdb():
    db = KVDB(visited_json)
    df1 = read_cluster_csv('c:/Users/aland/class/DSC650/dsc650/data/external/tidynomicon/visited.csv')
    df = df1.fillna(" ")
    grouped_df = df.groupby(["visit_id","site_id"])
    for visit_id, group_df in grouped_df:
        #print(visit_id)
        db.set_value(visit_id, group_df.to_dict(orient='records')[0])
        print(db.get_value(visit_id))
    db.saveu()

def create_measurements_kvdb():
    db = KVDB(measurements_json)
    df1 = read_cluster_csv('c:/Users/aland/class/DSC650/dsc650/data/external/tidynomicon/measurements.csv')
    df = df1.fillna(" ")
    grouped_df = df.groupby(["visit_id","person_id","quantity"])
    for visit_id, group_df in grouped_df:
        #print(visit_id)
        db.set_value(visit_id, group_df.to_dict(orient='records')[0])
        print(db.get_value(visit_id))
    db.saveu()

create_sites_kvdb()
create_people_kvdb()
create_visits_kvdb()
create_measurements_kvdb()
