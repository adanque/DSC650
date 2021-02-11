"""
Author:     Alan Danque
Date:       20201208
Class:      DSC 650
Exercise:   3
"""
from iteration_utilities import unique_everseen
import os
import sys
import gzip
import json
from pathlib import Path
import csv
import avro.schema
import pandas as pd
import s3fs
# from fs_s3fs import S3FS
import pyarrow as pa
from pyarrow.json import read_json
import pyarrow.parquet as pq
#import fastavro
from fastavro.schema import load_schema
from fastavro import writer, reader, parse_schema
#import pygeohashpisr d
import pygeohash
import snappy
import jsonschema
from jsonschema import validate

from jsonschema.exceptions import ValidationError

# Correct snappy
# conda install -c conda-forge python-snappy

# Not online and available
#endpoint_url ='https://storage.budsc.midwest-datascience.com'
# aiohttp.client_exceptions.ClientConnectorError: Cannot connect to host storage.budsc.midwest-datascience.com:443 ssl:default [getaddrinfo failed]

endpoint_url = 'C:/Users/aland/class/DSC650/dsc650'
current_dir = Path(os.getcwd()).absolute()sogeti-capgemini
schema_dir = current_dir.joinpath('schemas')
results_dir = current_dir.joinpath('results')
results_dir.mkdir(parents=True, exist_ok=True)


def read_jsonl_data():
    f_gz = 'C:/Users/aland/class/DSC650/dsc650/data/processed/openflights/routes.jsonl.gz'
    with gzip.open(f_gz, 'rb') as f:
        records = [json.loads(line) for line in f.readlines()]

    return records

    #s3 = s3fs.S3FileSystem(
    #    anon=True,
    #    client_kwargs={
    #        'endpoint_url': endpoint_url
    #    }
    #)
    #s3 = s3fs.S3FileSystem(anon=False)
    #src_data_path = '../../../../data/processed/openflights/routes.jsonl.gz'
    #with s3.open(src_data_path, 'rb') as f_gz:
    #    with gzip.open(f_gz, 'rb') as f:
    #        records = [json.loads(line) for line in f.readlines()]

#read_jsonl_data()
records = read_jsonl_data()
#print(records)
#with open('./routes_out.json','w') as f:
#   Export json
#   json.dump(records, f, indent=2, sort_keys=True)

# 3.1.a JSON Schema
def validate_jsonl_data(records):
    schema_path = schema_dir.joinpath('routes-schema.json')
    with open(schema_path) as f:
        _schema = json.load(f)

    print(_schema)

    validation_csv_path = results_dir.joinpath('validation-results.csv')
    with open(validation_csv_path, 'w') as f:
        for i, record in enumerate(records):
            try:
                ## TODO: Validate record
                jsonschema.validate(record, _schema)
                ##pass
            except ValidationError as e:
                ## Print message if invalid record
                detail = e.message
                print(detail)
                f.write(str(e.path))
                f.write(str(e.instance))
                f.write(str(detail))
                return detail

validate_jsonl_data(records)

## 3.1.b Avro
def create_avro_dataset(records):
    schema_path = schema_dir.joinpath('routes.avsc')
    data_path = results_dir.joinpath('routes.avro')
    ## TODO: Use fastavro to create Avro dataset
    parsed_schema = load_schema(schema_path)
    with open(data_path, 'wb') as out:
        writer(out, parsed_schema, records)
        
create_avro_dataset(records)


## 3.1.c Parquet
def create_parquet_dataset():
    src_data_path = 'C:/Users/aland/class/DSC650/dsc650/data/processed/openflights/routes.jsonl.gz'
    parquet_output_path = results_dir.joinpath('routes.parquet')
    """
    # Bypassed 
    s3 = s3fs.S3FileSystem(
        anon=True,
        client_kwargs={
            'endpoint_url': endpoint_url
        }
    )
    """

    with gzip.open(src_data_path, 'rb') as f:
        records = [json.loads(line) for line in f.readlines()]
    #print(records)
    #print(type(records))
    df = pd.DataFrame(records)
    #with s3.open(src_data_path, 'rb') as f_gz:
    #    with gzip.open(f_gz, 'rb') as f:
        #pass
        ## TODO: Use Apache Arrow to create Parquet table and save the dataset
    table = pa.Table.from_pandas(df)
    print(table)
    pq.write_table(table, parquet_output_path, compression='none')

create_parquet_dataset()



## 3.1.d Protocol Buffers
sys.path.insert(0, os.path.abspath('routes_pb2'))

import routes_pb2
def _airport_to_proto_obj(airport):
    obj = routes_pb2.Airport()
    if airport is None:
        return None
    if airport.get('airport_id') is None:
        return None

    obj.airport_id = airport.get('airport_id')
    if airport.get('name'):
        obj.name = airport.get('name')
    if airport.get('city'):
        obj.city = airport.get('city')
    if airport.get('iata'):
        obj.iata = airport.get('iata')
    if airport.get('icao'):
        obj.icao = airport.get('icao')
    if airport.get('altitude'):
        obj.altitude = airport.get('altitude')
    if airport.get('timezone'):
        obj.timezone = airport.get('timezone')
    if airport.get('dst'):
        obj.dst = airport.get('dst')
    if airport.get('tz_id'):
        obj.tz_id = airport.get('tz_id')
    if airport.get('type'):
        obj.type = airport.get('type')
    if airport.get('source'):
        obj.source = airport.get('source')

    obj.latitude = airport.get('latitude')
    obj.longitude = airport.get('longitude')

    return obj


def _airline_to_proto_obj(airline):
    obj = routes_pb2.Airline()
    ## TODO: Create an Airline obj using Protocol Buffers API
    if airline is None:
        return None
    if airline.get('airline_id') is None:
        return None

    obj.airline_id = airline.get('airline_id')
    if airline.get('name'):
        obj.name = airline.get('name')
    if airline.get('alias'):
        obj.alias = airline.get('alias')
    if airline.get('iata'):
        obj.iata = airline.get('iata')
    if airline.get('icao'):
        obj.icao = airline.get('icao')
    if airline.get('callsign'):
        obj.callsign = airline.get('callsign')
    if airline.get('country'):
        obj.country = airline.get('country')
    if airline.get('active'):
        obj.active = airline.get('active')

    return obj


def create_protobuf_dataset(records):
    routes = routes_pb2.Routes()
    # get list of all fields

    for record in records:
        route = routes_pb2.Route()

        ## TODO: Implement the code to create the Protocol Buffers Dataset
        for key, value in record.items():
            if key=='airline':
                airline = _airline_to_proto_obj(value)
                airin = route.airline
                airin.name = airline.name
                airin.airline_id = airline.airline_id
                airin.active = airline.active
            if key=='src_airport' and value is not None:
                src_airport = _airport_to_proto_obj(value)
                srcairin = route.src_airport
                srcairin.name = src_airport.name
                srcairin.airport_id = src_airport.airport_id
                srcairin.latitude = src_airport.latitude
                srcairin.longitude = src_airport.longitude

            if key=='dst_airport' and value is not None:
                dst_airport = _airport_to_proto_obj(value)
                dstairin = route.dst_airport
                dstairin.name = dst_airport.name
                dstairin.airport_id = dst_airport.airport_id
                dstairin.latitude = dst_airport.latitude
                dstairin.longitude = dst_airport.longitude

            if key=='codeshare':
                route.codeshare = value
        routes.route.append(route)

    data_path = results_dir.joinpath('routes.pb')
    with open(data_path, 'wb') as f:
        byestAsString = routes.SerializeToString()
        f.write(byestAsString)

    #compressed_path = results_dir.joinpath('routes.pb.snappy')
    #with open(compressed_path, 'wb') as f:
    #    f.write(snappy.compress(routes.SerializeToString()))


create_protobuf_dataset(records)



## 3.2
## 3.2.a Simple Geohash Index
def create_hash_dirs(records):
    geoindex_dir = results_dir.joinpath('geoindex')
    geoindex_dir.mkdir(exist_ok=True, parents=True)
    hashes = []
    ## TODO: Create hash index
    for record in records:
        for key, value in record.items():
            if key == 'src_airport' and value is not None:
                geohval = pygeohash.encode(value['latitude'], value['longitude'])
                geohval1 = str(geohval)[0]
                geohval2 = str(geohval)[0:2]
                geohval3 = str(geohval)[0:3]+".jsonl.gz"
                geoindex_dir = results_dir.joinpath('geoindex')
                geoindex_1dir = geoindex_dir.joinpath(geohval1)
                geoindex_1dir.mkdir(parents=True, exist_ok=True)
                geoindex_2dir = geoindex_1dir.joinpath(geohval2)
                geoindex_2dir.mkdir(parents=True, exist_ok=True)
                jsonfilename = geoindex_2dir.joinpath(geohval3)
                with gzip.GzipFile(jsonfilename, 'w') as fout:
                    fout.write(json.dumps(value).encode('utf-8'))

create_hash_dirs(records)



## 3.2.b Simple Search Feature
def airport_search(latitude, longitude, distm):
    ## TODO: Create simple search to return nearest airport
    distm = distm / 1000
    srcgeoval = pygeohash.encode(latitude, longitude, precision=12)
    AirportDistances = []
    recout = []
    # traverse for all elements get unique src airports
    for record in records:
        for key, value in record.items():
            if key == 'src_airport' and value is not None:
                if value not in recout:
                    recout.append(value)

    #print(len(recout))
    for record in recout:
        dstname = record['name']
        dstlat = record['latitude']
        dstlong = record['longitude']
        geohval = pygeohash.encode(dstlat, dstlong, precision=12)
        distm_dstgeo1 = pygeohash.geohash_approximate_distance(srcgeoval, geohval) / 1000
        airport_dist = {
            "Airport": dstname,
            "Geoval": geohval,
            "Latitude": dstlat,
            "Longitude": dstlong,
            "Distance(m)": distm_dstgeo1
        }
        AirportDistances.append(airport_dist)

    AirportDistancesOut = list(unique_everseen(AirportDistances))
    print("The following airports are within "+str(distm)+" meters from the coordinates:")
    print("Latitude: "+str(latitude))
    print("Longitude: "+str(longitude))
    for i in range(len(AirportDistancesOut)):
        for k, v in AirportDistancesOut[i].items():
            if k == 'Distance(m)':
                if v <= distm: # and v != 0:
                    print(AirportDistancesOut[i])
    ##pass

distm = 625441
airport_search(41.1499988, -95.91779, distm)


