"""
Author:     Alan Danque
Date:       20201203
Class:      DSC 650
Exercise:   2.2
"""

from pathlib import Path
import json
import os
import pickle
from tinydb import TinyDB, Query

current_dir = Path(os.getcwd()).absolute()
results_dir = current_dir.joinpath('results')
kv_data_dir = results_dir.joinpath('kvdb')
kv_data_dir.mkdir(parents=True, exist_ok=True)

class DocumentDB(object):
    def __init__(self, db_path):
        people_json = kv_data_dir.joinpath('people.pickle')
        visited_json = kv_data_dir.joinpath('visited.pickle')
        sites_json = kv_data_dir.joinpath('sites.pickle')
        measurements_json = kv_data_dir.joinpath('measurements.pickle')

        self._db_path = Path(db_path)
        self._db = None

        ## TODO: Code Implemented Here
        with open(people_json, 'rb') as handle:
            self.people_json_data = pickle.load(handle)

        with open(visited_json, 'rb') as handle:
            self.visited_json_data = pickle.load(handle)

        with open(sites_json, 'rb') as handle:
            self.sites_json_data = pickle.load(handle)

        with open(measurements_json, 'rb') as handle:
            self.measurements_json_data = pickle.load(handle)

        self._load_db()

    def Merge(dict1, dict2):
        res = {**dict1, **dict2}
        return res

    def _load_db(self):
        self._db = TinyDB(self._db_path)
        table1 = self._db.table('PatientRecord')
        ## TODO: Code Implemented Here

        def Merge(dict1, dict2):
            res = {**dict1, **dict2}
            return res

        Q1 = Query()
        for key1, value1 in self.people_json_data.items():
            print("   ")
            personrow = value1
            #print(personrow) # PERSON
            person_visits = []
            person = {}

            for visit_key, visit_value in self.visited_json_data.items():
                measurements = []
                #print(visit_key)
                #print(visit_key[1])
                for site_key, site_value in self.sites_json_data.items():
                    if site_key == visit_key[1]:
                        sitek = site_key
                        site = site_value

                for measurement_key, measurement_value2 in self.measurements_json_data.items():
                    for measure_id, value in measurement_value2.items():
                        if measure_id == 'person_id' and value == key1 and visit_key[0] == measurement_key[0]:
                            visits = []
                            visits.append(visit_value)
                            measurements.append(measurement_value2)

                while ("" in measurements):
                    measurements.remove("")
                measurements_out = {}
                site_out = {}
                if len(measurements)!=0:
                    site_out["site"]=site
                    measurements_out["measurements"]=measurements
                    vs = Merge(visits[0], site_out)
                    vs = Merge(vs, measurements_out)
                    visits_list = [vs]
                    person_visits.append(visits_list)
            p_v_out = {"visits": person_visits}
            p_v_out = Merge(personrow, p_v_out)
            table1.insert(p_v_out)

        # Test Queries
        print(table1.search(Q1.person_id == 'dyer'))
        for item in table1:
                print(item)
        print(table1.all())

db_path = results_dir.joinpath('patient-info.json')
if db_path.exists():
    os.remove(db_path)

db = DocumentDB(db_path)