"""
Author:     Alan Danque
Date:       20210110
Class:      DSC 650
Exercise:   8
Purpose
    Load the parquet data from the t="Seconds" directory to partitioned folders within the results/stream/input path.
    Loops on time to load time related parquet partition from input path to partition named folder within the results/stream/staging path.
    Calculate a timestamp column named datetimeoffset that adds the seconds part of the partition named to the time when the simulation/program started folder to the dataframe
    Writes the updated dataframe to the parquet partition named folder within the results/stream/staging path using spark.
    Move the files from the staging directory to the input directory.
"""

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

def loadParquet(results_dir):
    parquet_file = results_dir.joinpath('routes.parquet')
    new_parquet_file = results_dir.joinpath('routes_new.parquet')

    stream_dir = results_dir.joinpath('stream')
    stream_dir.mkdir(parents=True, exist_ok=True)

    input_dir = stream_dir.joinpath('input')
    input_dir.mkdir(parents=True, exist_ok=True)

    # partitioned_parquet_file = results_dir.joinpath('hash')
    accel_input_parquet_dir = input_dir.joinpath('accelerations')
    accel_input_parquet_dir.mkdir(parents=True, exist_ok=True)
    # Clear input folders
    try:
        shutil.rmtree(accel_input_parquet_dir)
    except OSError as e:
        print("Error: %s : %s" % (file_path, e.strerror))
    accel_input_parquet_dir.mkdir(parents=True, exist_ok=True)

    output_dir = stream_dir.joinpath('output')
    output_dir.mkdir(parents=True, exist_ok=True)

    staging_dir = stream_dir.joinpath('staging')
    #staging_dir.mkdir(parents=True, exist_ok=True)

    csv_file = results_dir.joinpath('routes_parquet.csv')
    data_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment08/').joinpath('Data')

    #print(parquet_file)
    # Works however will attempt to read using spark
    pq = pd.read_parquet(parquet_file, engine='fastparquet')

    # SPARK WORKS HOWEVER HAVING ISSUES WITH passing variablized path
    #pqr = spark.read.parquet('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment08/results/routes.parquet') #.toPandas()
    #pq = pqr #.compute()
    #readparquet_file = Path(parquet_file)
    #pq = spark.read.parquet(readparquet_file) #

    # Simplify dataframe and create the seconds in time partitions
    pq['group_1'] = np.arange(len(pq)) // 600
    pq['group_10'] = pq['group_1'].astype(str)
    pq['group_2'] = pq['group_10'].apply(lambda x: x.zfill(3))
    pq['group_3'] = np.random.randint(0, 10, pq.shape[0])
    pq['t'] = pq['group_2'] + "." + pq['group_3'].astype(str)
    pq['tid'] = pq['t']
    pq['airline_alias'] = pq['airline.alias']
    pq['airline_callsign'] = pq['airline.callsign']
    pq['airline_country'] = pq['airline.country']
    pq['airline_iata'] = pq['airline.iata']
    pq['airline_icao'] = pq['airline.icao']
    pq['airline_name'] = pq['airline.name']
    pq['dst_airport_name'] = pq['dst_airport.name']
    pq['src_airport_name'] = pq['src_airport.name']

    pq = pq.drop('group_1', 1)
    pq = pq.drop('group_10', 1)
    pq = pq.drop('group_2', 1)
    pq = pq.drop('group_3', 1)
    pq = pq.drop('airline.active', 1)
    pq = pq.drop('airline.airline_id', 1)
    pq = pq.drop('airline.alias', 1)
    pq = pq.drop('airline.callsign', 1)
    pq = pq.drop('airline.country', 1)
    pq = pq.drop('airline.iata', 1)
    pq = pq.drop('airline.icao', 1)
    pq = pq.drop('airline.name', 1)
    pq = pq.drop('codeshare', 1)
    pq = pq.drop('equipment', 1)
    pq = pq.drop('src_airport.airport_id', 1)
    pq = pq.drop('src_airport.altitude', 1)
    pq = pq.drop('src_airport.city', 1)
    pq = pq.drop('src_airport.country', 1)
    pq = pq.drop('src_airport.dst', 1)
    pq = pq.drop('src_airport.iata', 1)
    pq = pq.drop('src_airport.icao', 1)
    pq = pq.drop('src_airport.latitude', 1)
    pq = pq.drop('src_airport.longitude', 1)
    pq = pq.drop('src_airport.name', 1)
    pq = pq.drop('src_airport.source', 1)
    pq = pq.drop('src_airport.timezone', 1)
    pq = pq.drop('src_airport.type', 1)
    pq = pq.drop('src_airport.tz_id', 1)
    pq = pq.drop('dst_airport.airport_id', 1)
    pq = pq.drop('dst_airport.altitude', 1)
    pq = pq.drop('dst_airport.city', 1)
    pq = pq.drop('dst_airport.country', 1)
    pq = pq.drop('dst_airport.dst', 1)
    pq = pq.drop('dst_airport.iata', 1)
    pq = pq.drop('dst_airport.icao', 1)
    pq = pq.drop('dst_airport.latitude', 1)
    pq = pq.drop('dst_airport.longitude', 1)
    pq = pq.drop('dst_airport.name', 1)
    pq = pq.drop('dst_airport.source', 1)
    pq = pq.drop('dst_airport.timezone', 1)
    pq = pq.drop('dst_airport.type', 1)
    pq = pq.drop('dst_airport.tz_id', 1)

    # Cleanup drop rows with NaN, \N and nan
    pq = pq.dropna(how='any')
    pq = pq.drop(pq[pq.airline_alias == '\\N'].index)
    pq = pq.drop(pq[pq.airline_alias == 'nan'].index)

    # Created For troubleshooting the complete contents of the dataframe
    pq.to_csv(csv_file, sep='\t')
    table = pa.Table.from_pandas(pq)

    # Create the new partitioned parquet dataset to be later read
    parq.write_to_dataset(
        table,
        root_path=accel_input_parquet_dir,
        partition_cols=['t'],
    )

    # Clears the staging folder
    staging_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.rmtree(staging_dir)
    except OSError as e:
        print("Error: %s : %s" % (file_path, e.strerror))



def startTimedParquetStreamUpdateLoop(results_dir):
    # Uses spark to write the updated parquet
    stream_dir = results_dir.joinpath('stream')
    input_dir = stream_dir.joinpath('input')
    accel_input_parquet_dir = input_dir.joinpath('accelerations')

    staging_dir = stream_dir.joinpath('staging')
    staging_dir.mkdir(parents=True, exist_ok=True)

    accel_staging_parquet_dir = staging_dir.joinpath('accelerations')
    accel_staging_parquet_dir.mkdir(parents=True, exist_ok=True)
    locations_staging_parquet_dir = staging_dir.joinpath('locations')
    locations_staging_parquet_dir.mkdir(parents=True, exist_ok=True)
    std = (time.time() - start_time)
    before, after = str(std).split('.')
    tval_parquet = "t=" + before.zfill(3) + "." + after[:1]

    accel_input_parquet_read_dir = accel_input_parquet_dir.joinpath(tval_parquet)
    checkavail = os.path.exists(accel_input_parquet_read_dir)
    if checkavail == True:
        print("Processing partition: " + tval_parquet + "from: ")
        print(accel_input_parquet_read_dir)
        updated_parquet_file = accel_staging_parquet_dir.joinpath(tval_parquet)

        dataset = dd.read_parquet(accel_input_parquet_read_dir)
        print("Before offset column add")
        print(type(dataset))
        # Convert dask dataframe to pandas dataframe
        dataset = dataset.compute()
        print(type(dataset))

        dataset['t']=dataset['tid']
        dataset = dataset.drop('tid', 1)

        print(dataset)
        print(dataset.shape)
        print(dataset.ndim)
        print(dataset.dtypes)
        print(dataset.head())

        # GET SCHEMA
        print(dataset.info())

        # Create DateTimeOffset dataframe column
        secval= float(before +"."+ after)
        print("These Seconds")
        print(secval)
        dataset["DateTimeOffset"]=start_datetime + timedelta(seconds=secval)

        print("After offset column add")
        print(dataset.head())


        print("Writing to staging parquet folder")
        # table = pa.Table.from_pandas(dataset)
        # dataset.to_parquet(updated_parquet_file, engine='pyarrow') # Bypasses spark parquet write
        # For troubleshooting issues
        csv_file = results_dir.joinpath('routes_parquet_test_atd.csv')
        dataset.to_csv(csv_file, sep='\t')

        # Create Spark Dataframe and 1 partition parquet
        mySchema = StructType([StructField('airline_alias', StringType(), True)
                              , StructField('airline_callsign', StringType(), True)
                              , StructField('airline_country', StringType(), True)
                              , StructField('airline_iata', StringType(), True)
                              , StructField('airline_icao', StringType(), True)
                              , StructField('airline_name', StringType(), True)
                              , StructField('dst_airport_name', StringType(), True)
                              , StructField('src_airport_name', StringType(), True)
                              , StructField('t', StringType(), True)
                              , StructField('DateTimeOffset', TimestampType(), True)
                               ])
        print(mySchema)
        dataset = spark.createDataFrame(dataset, schema=mySchema)
        dataset.show()
        dataset.printSchema()

        print(updated_parquet_file)
        print(updated_parquet_file)
        print(accel_staging_parquet_dir)

        # Issue with passing variablized path value
        #dataset.repartition(1).write.partitionBy("t").parquet('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment08/results/stream/staging/accelerations/' + tval_parquet + '/part.0.parquet/')
        parquetwritepath = 'C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment08/results/stream/staging/accelerations/' + tval_parquet
        dataset.repartition(1).write.parquet(parquetwritepath)
        print("spark written parquet complete")

        # Move from the staging to the input folder
        print("Moving from staging to input folder")
        # Get name of the parquet file to swap with part.0.parquet
        source_dir = updated_parquet_file
        target_dir = accel_input_parquet_read_dir
        targetparqfilenames = os.listdir(target_dir)
        for file_name in targetparqfilenames:
            targetparqfilename=file_name
        print(targetparqfilename)
        print(updated_parquet_file)
        # Get the parquet file name
        globpath = 'C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment08/results/stream/staging/accelerations/' + tval_parquet+'/*.parquet'
        for fname in glob.glob(globpath):
            source_filename = fname
        print(source_filename)

        # Move parquet file
        shutil.move(os.path.join(source_dir, source_filename), os.path.join(target_dir, targetparqfilename))

        # Move the rest of the files
        file_names = os.listdir(source_dir)
        for file_name in file_names:
            shutil.move(os.path.join(source_dir, file_name), target_dir)

    # Return if the input folder was not available.
        retval = 0
    else:
        retval = 1
        print("Load partition does not exists: ", accel_input_parquet_read_dir)
    return retval

def startTimer(results_dir):
    # Loop on time
    print("call function here")
    retval = startTimedParquetStreamUpdateLoop(results_dir)
    # Stop if time is over and there are no more partitions.
    if ((time.time() - start_time) < 70 and retval == 0):
        t = threading.Timer(interval, startTimer(results_dir))


base_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment08/')
results_dir = base_dir.joinpath('results')
loadParquet(results_dir)
startTimer(results_dir)
# Show duration
print("Complete: --- %s seconds has passed ---" % (time.time() - start_time))