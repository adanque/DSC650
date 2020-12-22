"""
Author:     Alan Danque
Date:       20201203
Class:      DSC 650
Exercise:   2.3
"""

from ZODB import FileStorage, DB
import transaction
import pandas as pd
from pathlib import Path
import os

current_dir = Path(os.getcwd()).absolute()
results_dir = current_dir.joinpath('results')
results_dir.mkdir(parents=True, exist_ok=True)
patients_info = results_dir.joinpath('patient-info.fs')
patients_info_index = results_dir.joinpath('patient-info.fs.index')
if patients_info.exists():
    os.remove(patients_info)
if patients_info_index.exists():
    os.remove(patients_info_index)

patients_info_dir = patients_info.as_posix()

def read_cluster_csv(file_path):
    df = pd.read_csv(file_path)
    return df

class MyZODB(object):
    def __init__(self, path):
        self.storage = FileStorage.FileStorage(path)
        self.db = DB(self.storage)
        self.connection = self.db.open()
        self.dbroot = self.connection.root()
        self.create_sites()
        self.create_people()
        self.create_visits()
        self.create_measurements()
        # Review what is in the ZODB database
        print(self.dbroot.items())
        # Print just the sites object
        df = self.dbroot["sites"]
        print(type(df))
        print(df)
        self.close()

    def create_sites(self):
        df = read_cluster_csv('c:/Users/aland/class/DSC650/dsc650/data/external/tidynomicon/site.csv')
        #print(df)
        self.dbroot['sites'] = df
        transaction.commit()

    def create_people(self):
        df = read_cluster_csv('c:/Users/aland/class/DSC650/dsc650/data/external/tidynomicon/person.csv')
        #print(df)
        self.dbroot['people'] = df
        transaction.commit()

    def create_visits(self):
        df1 = read_cluster_csv('c:/Users/aland/class/DSC650/dsc650/data/external/tidynomicon/visited.csv')
        #print(df1)
        self.dbroot['visits'] = df1
        transaction.commit()

    def create_measurements(self):
        df1 = read_cluster_csv('c:/Users/aland/class/DSC650/dsc650/data/external/tidynomicon/measurements.csv')
        #print(df1)
        self.dbroot['measurements'] = df1
        transaction.commit()

    def close(self):
        self.connection.close()
        self.db.close()
        self.storage.close()

MyZODB(patients_info_dir)
