"""
Author:     Alan Danque
Date:       20210110
Class:      DSC 650
Exercise:   9.2
Purpose

"""

import os
import shutil
import json
from pathlib import Path
import time
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin.new_topic import NewTopic
from kafka.errors import TopicAlreadyExistsError

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
from pyspark.sql.functions import window, from_json, col
from pyspark.sql.types import StringType, TimestampType, DoubleType, StructField, StructType
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import mean
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
spark = (SparkSession
         .builder
         .appName('changes-event-consumer')
         .config("spark.jars.packages", "org.apache.kafka:kafka-clients:0.10.1.0") \
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5") \
         .master("local[*]").config(conf=scc).getOrCreate())

# Speeds up spark
#spark.conf.set("spark.sql.execution.arrow.enabled", "true") # 70seconds
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # 70seconds with 71.5 seconds without both
spark.conf.set("spark.rapids.sql.format.parquet.read.enabled", "true")
spark.conf.set("spark.rapids.sql.format.parquet.write.enabled", "true")
spark.conf.set("spark.rapids.sql.format.parquet.reader.type=MULTITHREADED", "true")
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
spark.sql("SET spark.sql.streaming.metricsEnabled=true")

#sc = SparkContext("local", "Hello World App")

sc = spark.sparkContext
sqlContext = SQLContext(sc)
print(sqlContext)
print(type(sqlContext))


start_datetime = datetime.now()
start_time = time.time()
interval = .1


base_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment09/')
results_dir = base_dir.joinpath('results')

# 'C:/Users/aland/class/DSC650/dsc650/data/processed/bdd/accelerations/'
current_dir = Path('C:/Users/aland/class/DSC650/dsc650/data/processed/bdd/') #Path(os.getcwd()).absolute()
checkpoint_dir = current_dir.joinpath('checkpoints')
locations_windowed_checkpoint_dir = checkpoint_dir.joinpath('locations-windowed')

if locations_windowed_checkpoint_dir.exists():
    shutil.rmtree(locations_windowed_checkpoint_dir)

locations_windowed_checkpoint_dir.mkdir(parents=True, exist_ok=True)

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
config['windowed_topic'] = '{}-windowed'.format(config['topic_prefix'])

print(config)
producer = KafkaProducer(bootstrap_servers=config['bootstrap_servers'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
#general_consumer = KafkaConsumer(bootstrap_servers=config['bootstrap_servers'], consumer_timeout_ms=1000)


location_data = [
    ("1", "1", "1", "McDonalds", "200.15", "200.15", "200.15", "McDonalds", "-3104.15", "-3104.15"),
    ("2", "2", "2", "Wendys", "200.15", "200.15", "200.15", "Wendys", "200.15", "200.15"),
    ("3", "3", "3", "Red Robbin", "200.15", "200.15", "200.15", "Red Robbin", "200.15", "200.15"),
    ("4", "4", "4", "Burger King", "200.15", "200.15", "200.15", "Burger King", "200.15", "200.15"),
    ("5", "5", "5", "Culvers", "200.15", "200.15", "200.15", "Culvers", "200.15", "200.15"),
    ("6", "6", "6", "Portillos", "200.15", "200.15", "200.15", "Portillos", "200.15", "200.15"),
]

acceleration_data = [
    ("1", "1", "1", "McDonalds", "200.15", "200.15", "200.15"),
    ("2", "2", "2", "Wendys", "200.15", "200.15", "200.15"),
    ("3", "3", "3", "Red Robbin", "200.15", "200.15", "200.15"),
    ("4", "4", "4", "Burger King", "200.15", "200.15", "200.15"),
    ("5", "5", "5", "Culvers", "200.15", "200.15", "200.15"),
    ("6", "6", "6", "Portillos", "200.15", "200.15", "200.15")
]

print("dfl")
dfl = spark.createDataFrame(location_data, ["offset", "id", "ride_id", "uuid", "course", "latitude", "longitude"
    , "geohash", "speed", "accuracy"])
print(dfl.show())
print(type(dfl))

print("dfr")
dfr = spark.createDataFrame(acceleration_data, ["offset", "id", "ride_id", "uuid", "x", "y", "z"])
print(dfr.show())
print(type(dfr))


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

"""
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

"""

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
    import uuid
    topic_prefix = config['topic_prefix']
    topic_name = '{}-{}'.format(topic_prefix, topic)
    print(topic)
    print(topic_prefix)
    print(topic_name)

    if msg_key is not None:
        key = msg_key
    else:
        #key = uuid4.uuid4().hex
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


create_kafka_topic('locations')
create_kafka_topic('accelerations')
create_kafka_topic('windowed')

par_accelerations = json.loads(dfr.toJSON().first())
par_locations = json.loads(dfl.toJSON().first())

#par_accelerations = sqlContext.read().json(dfr.toJSON())
#par_locations = sqlContext.read().json(dfl.toJSON())

print(type(par_accelerations))
print(par_accelerations)

print(type(par_locations))
print(par_locations)

send_data('accelerations', par_accelerations)
send_data('locations', par_locations)
# Watch that it is sent
# C:\tools\kafka\bin\windows\kafka-console-consumer.bat --topic DanqueAlan-locations --from-beginning --bootstrap-server localhost:9092

df_locations = spark \
  .readStream \
  .format("org.apache.spark.sql.kafka010.KafkaSourceProvider") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("startingOffsets", "earliest") \
  .option("subscribe", config['locations_topic']) \
  .load()


print(df_locations)
print("printing schema-df_locations")
print(df_locations.printSchema())

print("Showing some rows")
query = df_locations.writeStream.format("console").start()
time.sleep(10) # sleep 10 seconds
query.stop()


## TODO: Add code to create the df_accelerations dataframe
df_accelerations = spark \
  .readStream \
  .format("org.apache.spark.sql.kafka010.KafkaSourceProvider") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", config['accelerations_topic']) \
  .load()


location_schema = StructType([
    StructField('offset', DoubleType(), nullable=True),
    StructField('id', StringType(), nullable=True),
    StructField('ride_id', StringType(), nullable=True),
    StructField('uuid', StringType(), nullable=True),
    StructField('course', DoubleType(), nullable=True),
    StructField('latitude', DoubleType(), nullable=True),
    StructField('longitude', DoubleType(), nullable=True),
    StructField('geohash', StringType(), nullable=True),
    StructField('speed', StringType(), nullable=True),
    StructField('accuracy', StringType(), nullable=True),
])

acceleration_schema = StructType([
    StructField('offset', DoubleType(), nullable=True),
    StructField('id', StringType(), nullable=True),
    StructField('ride_id', StringType(), nullable=True),
    StructField('uuid', StringType(), nullable=True),
    StructField('x', DoubleType(), nullable=True),
    StructField('y', DoubleType(), nullable=True),
    StructField('z', DoubleType(), nullable=True),
])




udf_parse_acceleration = udf(lambda x: json.loads(x.decode('utf-8')), acceleration_schema)
udf_parse_location = udf(lambda x: json.loads(x.decode('utf-8')), location_schema)

df_locations_parsed = df_locations \
  .select(col('timestamp'), df_locations['key'].alias('key'), df_locations['value'].alias('value'), udf_parse_location(df_locations['value']).alias('json_value'))

print("df_locations_parsed")
print(type(df_locations_parsed))
print(df_locations_parsed)
print(df_locations_parsed.printSchema())

df_locations_parsed = df_locations_parsed.select(
    col('timestamp'),
    col('key'),
    col('value'),
    col('json_value.ride_id').alias('ride_id'),
    col('json_value.uuid').alias('uuid'),
    col('json_value.speed').alias('speed'),
    col('json_value.accuracy').alias('accuracy')
  )

print("df_locations_parsed before WindowedSpeeds")
print(type(df_locations_parsed))
print(df_locations_parsed)
print(df_locations_parsed.printSchema())

print("Showing some rows - df_locations_parsed")
query = df_locations_parsed.writeStream.format("console").start()
time.sleep(10) # sleep 10 seconds
query.stop()

windowedSpeeds = df_locations_parsed \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(window(df_locations_parsed.timestamp, "30 seconds", "15 seconds"), df_locations_parsed.ride_id
             , df_locations_parsed.speed) \
    .agg(mean(df_locations_parsed.speed).alias("value"), mean(df_locations_parsed.ride_id).alias("key"))

print("windowedSpeeds")
print(type(windowedSpeeds))
print(windowedSpeeds)
print(windowedSpeeds.printSchema())


ds_locations_windowed = windowedSpeeds \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .outputMode("update") \
  .format("org.apache.spark.sql.kafka010.KafkaSourceProvider") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", config['windowed_topic']) \
  .option("checkpointLocation", str(locations_windowed_checkpoint_dir)) \
  .start()
#.option("checkpointLocation", str(locations_windowed_checkpoint_dir)) \
print("ds_locations_windowed")
print(type(ds_locations_windowed))
print(ds_locations_windowed)

try:
    ds_locations_windowed.awaitTermination()
except KeyboardInterrupt:
    print("STOPPING STREAMING DATA")


print("Complete: --- %s seconds has passed ---" % (time.time() - start_time))
