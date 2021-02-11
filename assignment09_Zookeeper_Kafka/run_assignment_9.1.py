"""
Author:     Alan Danque
Date:       20210110
Class:      DSC 650
Exercise:   9.1
Purpose
    Creates a kafka topic pipeline that uses readstream to writestream to another topic.
    Uses the following from Assignment 8 to readstream and writestream to the simple topic
    Load partitioned parquet data dependent on time t="Seconds". After 52.5 seconds loads partition at 52.5 and then sends via producer to the consumer.

Dependencies:

Start daemons
    1.
    Start Zookeeper
    C:\tools\zookeeper\bin\zkServer.cmd
    2.
    Start Kafka
    C:\Tools\kafka\bin\windows\kafka-server-start.bat C:\Tools\kafka\config\server.properties

    3. list topics
    C:\tools\kafka\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181

Versions
    Need to use the following versions in order to work.
    Kafka 2.7.0
    Scala 2.12.10
    Hadoop 2.7
    pyspark 2.4.6
    spark 2.4.7

pip install --force-reinstall pyspark==2.4.6

C:\Tools\kafka\bin\windows>spark-submit --version
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.7
      /_/

Using Scala version 2.11.12, Java HotSpot(TM) 64-Bit Server VM, 1.8.0_271
Branch HEAD
Compiled by user prashant on 2020-09-08T05:22:44Z
Revision 14211a19f53bd0f413396582c8970e3e0a74281d
Url https://prashant:Sharma1988%235031@gitbox.apache.org/repos/asf/spark.git
Type --help for more information.


"""


import uuid
from json import dumps
from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.admin.new_topic import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import KafkaError

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
from pyspark.sql.functions import window, from_json, col
from pyspark.sql.types import StringType, TimestampType, DoubleType, StructField, StructType
from pyspark.sql.functions import udf
import pandas as pd
from time import sleep
import decimal
import threading
import shutil
import os
import glob
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parq
import time
import dask.dataframe as dd
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import sys
from pyspark.sql.types import *



import sys

#from pyspark import SparkContext
#from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import SparkSession

scc = SparkConf()
threads_max = 512
connection_max = 600
scc.set("spark.driver.memory", "10g")
scc.set('spark.hadoop.fs.s3a.threads.max', threads_max)
scc.set('spark.hadoop.fs.s3a.connection.maximum', connection_max)
scc.set('spark.hadoop.fs.s3a.aws.credentials.provider',
           'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
scc.set('spark.driver.maxResultSize', 0)

# Spark session & context
"""
spark = SparkSession\
    .builder\
    .appName("Assignment09")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5") \
    .master("local[*]").config(conf=scc).getOrCreate())
"""
spark = (SparkSession
         .builder
         .appName('wiki-changes-event-consumer')
         .config("spark.jars.packages", "org.apache.kafka:kafka-clients:0.10.1.0") \
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5") \
         .master("local[*]").config(conf=scc).getOrCreate())

sc = spark.sparkContext
start_datetime = datetime.now()
start_time = time.time()
interval = .1

# Speeds up spark
#spark.conf.set("spark.sql.execution.arrow.enabled", "true") # 70seconds
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # 70seconds with 71.5 seconds without both
spark.conf.set("spark.rapids.sql.format.parquet.read.enabled", "true")
spark.conf.set("spark.rapids.sql.format.parquet.write.enabled", "true")
spark.conf.set("spark.rapids.sql.format.parquet.reader.type=MULTITHREADED", "true")
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
spark.sql("SET spark.sql.streaming.metricsEnabled=true")

base_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment09/')
results_dir = base_dir.joinpath('results')

# 'C:/Users/aland/class/DSC650/dsc650/data/processed/bdd/accelerations/'
current_dir = Path('C:/Users/aland/class/DSC650/dsc650/data/processed/bdd/') #Path(os.getcwd()).absolute()
checkpoint_dir = current_dir.joinpath('checkpoints')
locations_checkpoint_dir = checkpoint_dir.joinpath('locations')
accelerations_checkpoint_dir = checkpoint_dir.joinpath('accelerations')

if locations_checkpoint_dir.exists():
    shutil.rmtree(locations_checkpoint_dir)

if accelerations_checkpoint_dir.exists():
    shutil.rmtree(accelerations_checkpoint_dir)

locations_checkpoint_dir.mkdir(parents=True, exist_ok=True)
accelerations_checkpoint_dir.mkdir(parents=True, exist_ok=True)


config = dict(
    bootstrap_servers=['localhost:9092'],
    first_name='Alan',
    last_name='Danque'
)

config['client_id'] = '{}{}'.format(
    config['last_name'],
    config['first_name']
)
config['topic_prefix'] = '{}{}'.format(
    config['last_name'],
    config['first_name']
)
config['locations_topic'] = '{}-locations'.format(config['topic_prefix'])
config['accelerations_topic'] = '{}-accelerations'.format(config['topic_prefix'])
config['simple_topic'] = '{}-simple'.format(config['topic_prefix'])

print("Printing Config")
print(config)
producer = KafkaProducer(bootstrap_servers=config['bootstrap_servers'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
general_consumer = KafkaConsumer(bootstrap_servers=config['bootstrap_servers'], consumer_timeout_ms=1000)

def create_kafka_topic(topic_name, config=config, num_partitions=1, replication_factor=1):
    bootstrap_servers = config['bootstrap_servers']
    client_id = config['client_id']
    topic_prefix = config['topic_prefix']
    name = '{}-{}'.format(topic_prefix, topic_name)

    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id
    )

    topic = NewTopic(
        name=name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    topic_list = [topic]
    try:
        admin_client.create_topics(new_topics=topic_list)
        print('Created topic "{}"'.format(name))
    except TopicAlreadyExistsError as e:
        print('Topic "{}" already exists'.format(name))

# Create if not exists
create_kafka_topic('simple')

df_locations = spark \
  .readStream \
  .format("org.apache.spark.sql.kafka010.KafkaSourceProvider") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", config['locations_topic']) \
  .load()

print(df_locations)
print("printing schema-df_locations")
print(df_locations.printSchema())

ds_locations = df_locations.selectExpr("CAST(value AS STRING)")
print(type(ds_locations))

ds_locations = df_locations \
  .writeStream \
  .format("org.apache.spark.sql.kafka010.KafkaSourceProvider") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", config['simple_topic']) \
  .option("checkpointLocation", locations_checkpoint_dir) \
  .start()

df_accelerations = spark \
  .readStream \
  .format("org.apache.spark.sql.kafka010.KafkaSourceProvider") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", config['accelerations_topic']) \
  .load()
#  .option("startingOffsets", "earliest") \

print(df_accelerations)
print("printing schema-df_accelerations")
print(df_accelerations.printSchema())

ds_accelerations = df_accelerations.selectExpr("CAST(value AS STRING)")
print(type(ds_accelerations))

ds_accelerations = ds_accelerations \
  .writeStream \
  .format("org.apache.spark.sql.kafka010.KafkaSourceProvider") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", config['simple_topic']) \
  .option("checkpointLocation", accelerations_checkpoint_dir) \
  .start()


try:
    ds_locations.awaitTermination()
    ds_accelerations.awaitTermination()
except KeyboardInterrupt:
    print("STOPPING STREAMING DATA")


"""

DataFrame[key: binary, value: binary, topic: string, partition: int, offset: bigint, timestamp: timestamp, timestampType: int]
printing schema-df_locations
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)

"""
#query = df_locations.selectExpr("CAST(value AS STRING)").writeStream.outputMode("append").format("console").start()
#query.awaitTermination()


print("Complete: --- %s seconds has passed ---" % (time.time() - start_time))
