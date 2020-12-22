"""
Author:     Alan Danque
Date:       20201216
Class:      DSC 650
Exercise:   4.3

So to be able to view at http://localhost:8082/static/visualiser/index.html
started with "luigid"
"""

# (base) C:\Users\aland\class\DSC650\dsc650\dsc650\assignments\assignment04>
# python ./run_assignment_4.3_TEST.py --scheduler-host localhost ProcessEnronEmailsluigi --emails-directory "C:/Users/aland/class/DSC650/dsc650/data/external/enron/davis-d/" --processed-directory "C:/Users/aland/class/DSC650/dsc650/results/words/"

# Reminder
# Clear this folder first: C:\Users\aland\class\DSC650\dsc650\results\words

import luigi
import time
import os
import json
from pathlib import Path
from pyspark import SparkContext
sc =SparkContext()
import zipfile
import email
from email.policy import default
from email.parser import Parser
from datetime import timezone
from collections import namedtuple
import gzip
from collections import OrderedDict
import pandas as pd
import s3fs
from bs4 import BeautifulSoup
from dateutil.parser import parse
from chardet.universaldetector import UniversalDetector

from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.pipeline import Transformer
from pyspark.sql.functions import udf
#from pyspark.sql.types import StructType, StringType
from pyspark.sql.types import *

import pandas as pd

# luigi --port 8082
# python ./run_assignment_4.2.py --scheduler-host localhost ProcessEnronEmails


#current_dir = Path(os.getcwd()).absolute()

current_dir = Path('C:/Users/aland/class/DSC650/dsc650/')
results_dir = current_dir.joinpath('results')
results_dir.mkdir(parents=True, exist_ok=True)

words_dir = results_dir.joinpath('words')
words_dir.mkdir(parents=True, exist_ok=True)

data_dir = current_dir.joinpath('data')
data_dir.mkdir(parents=True, exist_ok=True)
processed_directory = data_dir.joinpath('processed/enron')
processed_directory.mkdir(parents=True, exist_ok=True)
processed_directory = str(processed_directory)
enron_data_dir = str(data_dir.joinpath('external/enron'))

#print(data_dir)
#print(enron_data_dir)

emails_directory = enron_data_dir


class ProcessMailbox(luigi.Task):
    mailbox_directory = luigi.Parameter()
    processed_directory = luigi.Parameter()
    #processed_directory.mkdir(parents=True, exist_ok=True)

    def requires(self):
        return None

    def output(self):
        pass

    def run(self):
        #pass
        print('in class ProcessMailbox')
        print(self.processed_directory)
        emailfolders = self.mailbox_directory
        emailowner = os.path.basename(os.path.normpath(self.mailbox_directory))
        wordsfile = os.path.join(self.processed_directory, emailowner)
        wordsfilewext = os.path.join(self.processed_directory, emailowner+"_wrk")
        wordsfilewextout = os.path.join(self.processed_directory, emailowner + ".txt")
        print(emailowner)
        print(emailfolders)
        emaildirs = []
        #print("emails_directory")
        #print(emails_directory)
        ignored = {""}
        #print(type(emailfolders))
        for file in os.listdir(emailfolders):
            #print(file)
            d = os.path.join(emailfolders, file)
            if os.path.isdir(d):
                if d not in ignored:
                    emaildirs.append(d)
        #print(emaildirs)

        #totalcounts
        #Writes all files from all mailbox folders into one
        for directory in emaildirs:
            #print("directory")
            with open(wordsfile, 'w') as nf:
                for file in os.listdir(directory):
                    #print(file)
                    rfile = os.path.join(directory, file)
                    #print(rfile)
                    with open(rfile, 'r') as f:
                        # read all lines of a file and write into new file
                        lines_in_file = f.readlines()
                        nf.writelines(lines_in_file)
                        # insert a newline after reading each file
                        nf.write("\n")

            #prelimcounts
        #wordsfile
        text_file = sc.textFile(wordsfile)
        counts = text_file.flatMap(lambda line: line.split(" ")) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b)
        counts.saveAsTextFile(wordsfilewext)
        time.sleep(15)

class ProcessEnronEmails(luigi.WrapperTask):
    emails_directory = luigi.Parameter()
    processed_directory = luigi.Parameter()
    #print("emails_directory")
    #print(emails_directory)

    def requires(self):
        directories = []
        #print("emails_directory")
        #print(emails_directory)
        ignored = {"enron"}
        for file in os.listdir(emails_directory):
            d = os.path.join(emails_directory, file)
            if os.path.isdir(d):
                #print(d)
                if d not in ignored:
                    directories.append(d)

        #for root, dirs, files in os.walk(emails_directory):

        for directory in directories:
            #print("directory")
            print(directory)
            yield ProcessMailbox(
                mailbox_directory=str(directory),
                processed_directory=str(self.processed_directory)
            )

    def output(self):
        pass

    def run(self):
        print('in class ProcessEnronEmails')
        time.sleep(15)
        #pass

class ProcessEnronEmailsluigi(luigi.Task):
    emails_directory = luigi.Parameter()
    processed_directory = luigi.Parameter()
    #print("emails_directory")
    #print(emails_directory)

    def requires(self):
        directories = []
        #print("emails_directory")
        #print(emails_directory)
        ignored = {"enron"}
        for file in os.listdir(emails_directory):
            d = os.path.join(emails_directory, file)
            if os.path.isdir(d):
                #print(d)
                if d not in ignored:
                    directories.append(d)

        #for root, dirs, files in os.walk(emails_directory):

        for directory in directories:
            #print("directory")
            print(directory)
            yield ProcessMailbox(
                mailbox_directory=str(directory),
                processed_directory=str(self.processed_directory)
            )

    def output(self):
        pass

    def run(self):
        print('in class ProcessEnronEmails')
        time.sleep(15)
        #pass

"""
def main():
    tasks = [
        ProcessEnronEmails(emails_directory=emails_directory, processed_directory=words_dir)
    ]
    luigi.build(tasks, workers=8, local_scheduler=True)
"""

if __name__ == '__main__':
    luigi.run()
    #main()


