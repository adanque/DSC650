
"""
Author:     Alan Danque
Date:       20201216
Class:      DSC 650
Exercise:   4
"""
import luigi

import os
import json
from pathlib import Path
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
emails_directory = str(enron_data_dir)


print(processed_directory)
print(data_dir)
print(enron_data_dir)
print(words_dir)
print(type(processed_directory))
print(type(emails_directory))

def CombineCounts(emails_directory, processed_directory):

    #mailbox_directory = luigi.Parameter()
    #processed_directory = luigi.Parameter()
    wordsfile = os.path.join(processed_directory, "Combined.txt")
    directories = []
    ignored = {"enron"}
    for file in os.listdir(emails_directory):
        d = os.path.join(emails_directory, file)
        if os.path.isdir(d):
            if d not in ignored:
                directories.append(d)

    with open(wordsfile, 'w') as nf:
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
                print("Its a file")4r5r
                with open(wordsfile, 'r') as f:
                    # read all lines of a file and write into new file
                    lines_in_file = f.readlines()
                    #print(lines_in_file)
                    nf.writelines(lines_in_file)
                    # insert a newline after reading each file
                    nf.write("\n")



CombineCounts(emails_directory, words_dir)