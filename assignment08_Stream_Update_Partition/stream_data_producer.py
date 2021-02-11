"""
Author:     Alan Danque
Date:       20210110
Class:      DSC 650
Exercise:   8
Purpose
    Load partitioned parquet data dependent on time t="Seconds". After 52.5 seconds loads partition at 52.5 and then sends via producer to the consumer.

"""

import json
import uuid
from json import dumps
from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.admin.new_topic import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import KafkaError
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
spark = SparkSession.builder \
    .master('local') \
    .appName('parquetFile') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

start_datetime = datetime.now()
start_time = time.time()
interval = .1

# Speeds up spark
#spark.conf.set("spark.sql.execution.arrow.enabled", "true") # 70seconds
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # 70seconds with 71.5 seconds without both
spark.conf.set("spark.rapids.sql.format.parquet.read.enabled", "true")
spark.conf.set("spark.rapids.sql.format.parquet.write.enabled", "true")
spark.conf.set("spark.rapids.sql.format.parquet.reader.type=MULTITHREADED", "true")

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

print(config)
producer = KafkaProducer(bootstrap_servers=config['bootstrap_servers'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
general_consumer = KafkaConsumer(bootstrap_servers=config['bootstrap_servers'], consumer_timeout_ms=1000)


def loadParquet(parq_path):
    pqr = spark.read.parquet(parq_path)
    # Convert from spark dataframe to pandas dataframe
    pqr = pqr.toPandas()
    return pqr

def splitstr(std):
    before, after = str(std).split('.')
    return before, after

def startTimer(results_dir):
    # Loop on time
    print("call function here")
    retval = startTimedParquetStreamUpdateLoop(results_dir)
    # Stop if time is over and there are no more partitions.
    if ((time.time() - start_time) < 70 and retval == 0):
        t = threading.Timer(interval, startTimer(results_dir))

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


def create_kafka_consumer(topics, config=config):
    bootstrap_servers = config['bootstrap_servers']
    client_id = config['client_id']
    topic_prefix = config['topic_prefix']
    topic_list = ['{}-{}'.format(topic_prefix, topic) for topic in topics]

    return KafkaConsumer(
        *topic_list,
        client_id=client_id,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x)
    )

consumer = create_kafka_consumer(['locations', 'accelerations'])


def print_messages(consumer=consumer):
    try:

        for message in consumer:
            msg_metadata = 'Message metadata: {}:{}:{}'.format(
                message.topic, message.partition, message.offset
            )

            # obtain the last offset value
            topic = message.topic
            tp = TopicPartition(topic, 0)
            consumer.seek_to_end(tp)
            lastOffset = consumer.position(tp)

            if message.key is not None:
                msg_key = message.key.decode('utf-8')
            else:
                msg_key = ''
            msg_value = json.dumps(message.value, indent=2)
            msg_value = '\n'.join(['  {}'.format(value) for value in msg_value.split('\n')])

            print('Message metadata:')
            print('  Topic: {}'.format(message.topic))
            print('  Partition: {}'.format(message.partition))
            print('  Offset: {}'.format(message.offset))
            print('Message Key: {}'.format(msg_key))
            print('Message Value:')
            print(msg_value)
            print()
            if message.offset == lastOffset - 1:
                break

    except KeyboardInterrupt:
        print("STOPPING MESSAGE CONSUMER")



def on_send_success(record_metadata):
    print('Message sent:\n    Topic: "{}"\n    Partition: {}\n    Offset: {}'.format(
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset
    ))

def on_send_error(excp):
    print('I am an errback', exc_info=excp)
    # handle exception


def send_data(topic, data, config=config, producer=producer, msg_key=None):
    topic_prefix = config['topic_prefix']
    topic_name = '{}-{}'.format(topic_prefix, topic)
    print(topic)
    print(topic_prefix)
    print(topic_name)

    if msg_key is not None:
        key = msg_key
    else:
        key = uuid.uuid4().hex

    print(data)
    sendout = producer.send(topic_name, key=key.encode('utf-8'), value=data).add_callback(on_send_success).add_errback(on_send_error)

    # Block for 'synchronous' sends
    try:
        record_metadata = sendout.get(timeout=10)
    except KafkaError:
        # Decide what to do if produce request failed...
        log.exception()
        pass

    # Successful result returns assigned partition and offset
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


# Will create if not exists already
create_kafka_topic('locations')
create_kafka_topic('accelerations')

base_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment08/')
results_dir = base_dir.joinpath('results')

# Loop on time loading accelerations and locations
fpath = 'C:/Users/aland/class/DSC650/dsc650/data/processed/bdd/accelerations/'
targetparqfilenames = os.listdir(fpath)
for fname in targetparqfilenames:
    std = (time.time() - start_time)
    before, after = str(std).split('.')
    fname_secs = decimal.Decimal(fname.replace("t=",""))
    #print(fname_secs)
    while std < fname_secs:
        sleep(0.05)
        std = (time.time() - start_time)
        print('Looping til after... ', fname_secs)

    print(fname_secs)
    parqaccl = 'C:/Users/aland/class/DSC650/dsc650/data/processed/bdd/accelerations/'+str(fname)
    parqloc = 'C:/Users/aland/class/DSC650/dsc650/data/processed/bdd/locations/'+str(fname)

    if fname_secs == 52.5:
        # Producer
        print("At the 52.5 mark")
        par_accelerations = loadParquet(parqaccl)
        par_accelerations = par_accelerations.to_json()
        par_locations = loadParquet(parqloc)
        par_locations = par_locations.to_json()
        send_data('accelerations', par_accelerations)
        send_data('locations', par_locations)
        break
    print("Complete: --- %s seconds has passed ---" % (time.time() - start_time))



print("Complete: --- %s seconds has passed ---" % (time.time() - start_time))
