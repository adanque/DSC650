"""
Author:     Alan Danque
Date:       20201216
Class:      DSC 650
Exercise:   4.3

"""
import shutil
import glob
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
# current_dir = Path(os.getcwd()).absolute()

current_dir = Path('C:/Users/aland/class/DSC650/dsc650/')
results_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment04/').joinpath('results')
results_dir.mkdir(parents=True, exist_ok=True)

words_dir = results_dir.joinpath('words')
words_dir.mkdir(parents=True, exist_ok=True)

data_dir = current_dir.joinpath('data')
data_dir.mkdir(parents=True, exist_ok=True)
processed_directory = data_dir.joinpath('processed/enron')
processed_directory.mkdir(parents=True, exist_ok=True)
processed_directory = str(processed_directory)
enron_data_dir = str(data_dir.joinpath('external/enron'))
emails_directory = enron_data_dir

class ProcessMailbox(luigi.Task):
    mailbox_directory = luigi.Parameter()
    processed_directory = luigi.Parameter()
    #processed_directory.mkdir(parents=True, exist_ok=True)

    def requires(self):
        return None

    def output(self):
        #pass
        print("ProcessMailbox Output")

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
        ignored = {""}
        for file in os.listdir(emailfolders):
            d = os.path.join(emailfolders, file)
            if os.path.isdir(d):
                if d not in ignored:
                    emaildirs.append(d)

        #Writes all files from all mailbox folders into one
        for directory in emaildirs:
            #print("directory")
            with open(wordsfile, 'w') as nf:
                for file in os.listdir(directory):
                    #print(file)
                    rfile = os.path.join(directory, file)
                    #print(rfile)
                    if os.path.isfile(rfile):
                        with open(rfile, 'r') as f:
                            # read all lines of a file and write into new file
                            lines_in_file = f.readlines()
                            nf.writelines(lines_in_file)
                            # insert a newline after reading each file
                            nf.write("\n")

        text_file = sc.textFile(wordsfile)
        counts = text_file.flatMap(lambda line: line.split(" ")) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b)
        counts.saveAsTextFile(wordsfilewext)
        # Added so to monitor via http://localhost:8082/static/visualiser/index.html
        #time.sleep(15)

        print("Creating Output File")
        print(wordsfilewextout)
        keepfiles = []
        for file in glob.glob(wordsfilewext+'/part-*'):
            with open(wordsfilewextout, 'w') as nf:
                print(file)
                if os.path.isfile(file):
                    keepfiles.append(file)
                    with open(file, 'r') as f:
                        # read all lines of a file and write into new file
                        lines_in_file = f.readlines()
                        nf.writelines(lines_in_file)
                        # insert a newline after reading each file
                        nf.write("\n")
        shutil.rmtree(wordsfilewext)
        #os.remove(wordsfile)


class ProcessEnronEmails(luigi.WrapperTask):
    emails_directory = luigi.Parameter()
    processed_directory = luigi.Parameter()

    def requires(self):
        directories = []
        ignored = {"enron"}
        for file in os.listdir(emails_directory):
            d = os.path.join(emails_directory, file)
            if os.path.isdir(d):
                if d not in ignored:
                    directories.append(d)

        for directory in directories:
            #print("directory")
            print(directory)
            yield ProcessMailbox(
                mailbox_directory=str(directory),
                processed_directory=str(self.processed_directory)
            )

    def output(self):
        #pass
        print("ProcessEnronEmails Output")

    def run(self):
        print('ProcessEnronEmails Run')
        # Added so to monitor via http://localhost:8082/static/visualiser/index.html
        #time.sleep(15)
        #pass

class ProcessEnronEmailsluigi(luigi.Task):
    emails_directory = luigi.Parameter()
    processed_directory = luigi.Parameter()

    def requires(self):
        directories = []
        ignored = {"enron"}
        for file in os.listdir(emails_directory):
            d = os.path.join(emails_directory, file)
            if os.path.isdir(d):
                if d not in ignored:
                    directories.append(d)

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
        # Added so to monitor via http://localhost:8082/static/visualiser/index.html
        #time.sleep(15)
        #pass



def CombineCounts(emails_directory, processed_directory):

    #mailbox_directory = luigi.Parameter()
    #processed_directory = luigi.Parameter()
    combinedwordsfile = os.path.join(processed_directory, "Combined.txt")
    combinedcounts = os.path.join(results_dir, "count")
    combinedcountstxt = os.path.join(results_dir, "count.txt")
    #os.remove(combinedcounts)
    directories = []
    ignored = {"enron"}
    for file in os.listdir(emails_directory):
        d = os.path.join(emails_directory, file)
        if os.path.isdir(d):
            if d not in ignored:
                directories.append(d)

    with open(combinedwordsfile, 'w') as nf:
        for file in directories:
            print("directory")
            print(file)
            keepfiles = []
            #for file in glob.glob(wordsfilewext + '/part-*'):
            emailowner = os.path.basename(os.path.normpath(file))
            wordsfile = os.path.join(processed_directory, emailowner)
            #wordsfilewext = os.path.join(processed_directory, emailowner + "_wrk")
            #wordsfilewextout = os.path.join(processed_directory, emailowner + ".txt")
            print(emailowner)
            print(wordsfile)
            #print(wordsfilewext)
            #print(wordsfilewextout)
            if os.path.isfile(wordsfile):
                with open(wordsfile, 'r') as f:
                    # read all lines of a file and write into new file
                    lines_in_file = f.readlines()
                    #print(lines_in_file)
                    nf.writelines(lines_in_file)
                    # insert a newline after reading each file
                    nf.write("\n")
            os.remove(wordsfile)

    text_file = sc.textFile(combinedwordsfile)
    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile(combinedcounts)
    os.remove(combinedwordsfile)

    keepfiles = []
    for file in glob.glob(combinedcounts + '/part-*'):
        with open(combinedcountstxt, 'w') as nf:
            print(file)
            if os.path.isfile(file):
                keepfiles.append(file)
                with open(file, 'r') as f:
                    # read all lines of a file and write into new file
                    lines_in_file = f.readlines()
                    nf.writelines(lines_in_file)
                    # insert a newline after reading each file
                    nf.write("\n")
    shutil.rmtree(combinedcounts)

def main():
    tasks = [ProcessEnronEmails(emails_directory=emails_directory, processed_directory=words_dir)]
    luigi.build(tasks, workers=8, local_scheduler=True)
    #luigi.run()
    CombineCounts(emails_directory, words_dir)
    print("complete")

if __name__ == '__main__':
    main()


