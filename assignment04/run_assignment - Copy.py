
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
        #print(root)
        #print(dirs)
        #print(files)
        for file_path in files:
            #print(file_path)
            #print(current_path)
            ## Current path is now the file path to the current email.
            ## Use this path to read the following information
            ## original_msg
            ## username (Hint: It is the root folder)
            ## id (The relative path of the email message)
            current_path = Path(root).joinpath(file_path)
            #print(current_path)
            #print(read_raw_email(current_path))
            #data1 = pd.read_html(read_raw_email(current_path), skiprows=1)[0]
               # 'http://www.espn.com/nhl/statis‌​tics/player/‌​_/stat/point‌​s/sort/point‌​s/year/2015&‌​#47;seasontype/2‌​',
               # skiprows=1)[0]
            #print(data1)

            #soup = BeautifulSoup(data1, 'lxml')
            #print(soup)

            #d = OrderedDict()
            #for th, td in zip(soup.select('th'), soup.select('td')[::2]):
            #    d[th.text.strip()] = td.text.strip().splitlines()

            #print(d)

            #dfs = pd.read_html(html_string)
            #df = dfs[0]
            ## TODO: Complete the code to code to create the Spark dataframe
            fileName = current_path
            tokens = output_columns  # ['LastName', 'Color']
            #print(tokens)

            dictResult = {}
            with open(fileName, 'r') as fileHandle:
                for line in fileHandle:
                    # print(line)
                    lineParts = line.split(" ")
                    # print(lineParts)
                    if lineParts[0].replace(':', '') in tokens:  # len(lineParts) == 2 and
                        dictResult[lineParts[0]] = ' '.join(lineParts[1:]).replace('\n', ' ')

            #print(dictResult)
            records.append(dict(dictResult))
    # Convert list of dictionaries to a dataframe
    df = pd.DataFrame(records)
    #print(df)


    # Create PySpark DataFrame Schema
    p_schema = StructType([StructField('Date', StringType(), True), StructField('From', StringType(), True), StructField('To', StringType(), True), StructField('Subject', StringType(), True), StructField('Mime-Version', StringType(), True), StructField('Content-Type', StringType(), True), StructField('Content-Transfer-Encoding', StringType(), True), StructField('X-From', StringType(), True), StructField('X-To', StringType(), True), StructField('X-cc', StringType(), True), StructField('X-bcc', StringType(), True), StructField('X-Folder', StringType(), True), StructField('X-Origin', StringType(), True), StructField('X-FileName', StringType(), True)])

    """
Date
,StructField('From', StringType(), True)
,StructField('To', StringType(), True)
,StructField('Subject', StringType(), True)
,StructField('Mime-Version', StringType(), True)
,StructField('Content-Type', StringType(), True)
,StructField('Content-Transfer-Encoding', StringType(), True)
,StructField('X-From', StringType(), True)
,StructField('X-To', StringType(), True)
,StructField('X-cc', StringType(), True)
,StructField('X-bcc', StringType(), True)
,StructField('X-Folder', StringType(), True)
,StructField('X-Origin', StringType(), True)
,StructField('X-FileName', StringType(), True)


    
{
Date
From
To
Subject
Mime-Version
Content-Type
Content-Transfer-Encoding
X-From
X-To
X-cc
X-bcc
X-Folder
X-Origin
X-FileName
==============


'Date:': 'Sun, 23 Dec 2001 21:39:12 -0800 (PST) '
, 'From:': 'bluechipbrands2@m-ul.com '
, 'To:': 'jreitme@enron.com '
, 'Subject:': 'Find your Dream Mate!! '
, 'Mime-Version:': '1.0 '
, 'Content-Type:': 'text/plain; charset=us-ascii '
, 'Content-Transfer-Encoding:': '7bit '
, 'X-From:': 'Dream Mates <bluechipbrands2@m-ul.com> '
, 'X-To:': 'jreitme@enron.com '
, 'X-cc:': ' '
, 'X-bcc:': ' '
, 'X-Folder:': '\\James_Reitmeyer_Jan2002_1\\Reitmeyer, Jay\\Deleted Items '
, 'X-Origin:': 'Reitmeyer-J '
, 'X-FileName:': 'jreitme (Non-Privileged).pst '
}
    
    
    'payload'
    , 'text'
    , 'Message_D'
    , 'Date'
    , 'From'
    , 'To'
    , 'Subject'
    , 'Mime_Version'
    , 'Content_Type'
    , 'Content_Transfer_Encoding'
    , 'X_From'
    , 'X_To'
    , 'X_cc'
    , 'X_bcc'
    , 'X_Folder'
    , 'X_Origin'
    , 'X_FileName'
    , 'Cc'
    , 'Bcc']


    # Create Spark DataFrame from Pandas
    df_person = sqlContext.createDataFrame(pd_person, p_schema)
"""

    return spark.createDataFrame(df,p_schema)

#email_path='C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment04/examples/html/message-0.txt'
#print(read_raw_email(email_path))

sdf = make_spark_df()
print(sdf)


#data1 = pd.read_html(read_raw_email(current_path), skiprows=1)[0]
#data1 = pd.read_html(read_raw_email(email_path)) #, skiprows=1)[0]
#data1 = read_raw_email(email_path)
# 'http://www.espn.com/nhl/statis‌​tics/player/‌​_/stat/point‌​s/sort/point‌​s/year/2015&‌​#47;seasontype/2‌​',
# skiprows=1)[0]
#print(data1)

#soup = BeautifulSoup(data1, 'lxml')
#print(soup)



#d = OrderedDict()
#for th, td in zip(soup.select('th'), soup.select('td')[::2]):
#    d[th.text.strip()] = td.text.strip().splitlines()

#print(d)


#df = make_spark_df()
#df.show()
#df.printSchema()


