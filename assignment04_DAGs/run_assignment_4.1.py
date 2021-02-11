
"""
Author:     Alan Danque
Date:       20201216
Class:      DSC 650
Exercise:   4
"""
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

current_dir = Path(os.getcwd()).absolute()
results_dir = current_dir.joinpath('results')
results_dir.mkdir(parents=True, exist_ok=True)
data_dir = current_dir.joinpath('data')
data_dir.mkdir(parents=True, exist_ok=True)
#enron_data_dir = data_dir.joinpath('enron')
enron_data_dir = 'C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment04/examples/html/' #data_dir.joinpath('openflights')

# Assignment 4.1

f_gz = 'C:/Users/aland/class/DSC650/dsc650/data/processed/openflights/routes.jsonl.gz'
with gzip.open(f_gz, 'rb') as f:
    recordsout = [json.loads(line) for line in f.readlines()]

output_columns = [
        'payload',
        'text',
        'Message_D',
        'Date',
        'From',
        'To',
        'Subject',
        'Mime-Version',
        'Content-Type',
        'Content-Transfer-Encoding',
        'X-From',
        'X-To',
        'X-cc',
        'X-bcc',
        'X-Folder',
        'X-Origin',
        'X-FileName',
        'Cc',
        'Bcc'
]

columns = [column.replace('-', '_') for column in output_columns]
print(columns)

ParsedEmail = namedtuple('ParsedEmail', columns)
print(ParsedEmail)

spark = SparkSession\
    .builder\
    .appName("Assignment04")\
    .getOrCreate()


def read_raw_email(email_path):
    detector = UniversalDetector()

    try:
        with open(email_path) as f:
            original_msg = f.read()
    except UnicodeDecodeError:
        detector.reset()
        with open(email_path, 'rb') as f:
            for line in f.readlines():
                detector.feed(line)
                if detector.done:
                    break
        detector.close()
        encoding = detector.result['encoding']
        with open(email_path, encoding=encoding) as f:
            original_msg = f.read()

    return original_msg


def make_spark_df():
    records = []
    for root, dirs, files in os.walk(enron_data_dir):
        for file_path in files:
            current_path = Path(root).joinpath(file_path)
            ## TODO: Complete the code to code to create the Spark dataframe
            fileName = current_path
            tokens = output_columns  # ['LastName', 'Color']
            dictResult = {}
            with open(fileName, 'r') as fileHandle:
                for line in fileHandle:
                    #print(line)
                    lineParts = line.split(" ")
                    # print(lineParts)
                    if lineParts[0].replace(':', '') in tokens:  # len(lineParts) == 2 and
                        dictResult[lineParts[0]] = ' '.join(lineParts[1:]).replace('\n', ' ')

            #print(dictResult)
            records.append(dict(dictResult))
    #Convert list of dictionaries to a dataframe
    #print(records)
    df = pd.DataFrame(records)
    #print(df)
    # Create PySpark DataFrame Schema
    p_schema = StructType([StructField('Date', StringType(), True), StructField('From', StringType(), True), StructField('To', StringType(), True), StructField('Subject', StringType(), True), StructField('Mime-Version', StringType(), True), StructField('Content-Type', StringType(), True), StructField('Content-Transfer-Encoding', StringType(), True), StructField('X-From', StringType(), True), StructField('X-To', StringType(), True), StructField('X-cc', StringType(), True), StructField('X-bcc', StringType(), True), StructField('X-Folder', StringType(), True), StructField('X-Origin', StringType(), True), StructField('X-FileName', StringType(), True)])
    sdf = spark.createDataFrame(df, p_schema)
    #print(type(sdf))
    #print(sdf)
    return sdf

# TEST EMAIL READER
"""
email_path='C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment04/examples/html/message-0.txt'
print(read_raw_email(email_path))

email_path='C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment04/examples/plain/message-0.txt'
print(read_raw_email(email_path))
"""

#print(make_spark_df())
#sdf = make_spark_df()
#print(type(sdf))

df = make_spark_df()
df.show()
df.printSchema()


spark.stop()
print("done")

# Assignment 4.2
