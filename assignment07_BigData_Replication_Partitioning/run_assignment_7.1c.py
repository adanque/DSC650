"""
Author:     Alan Danque
Date:       20210101
Class:      DSC 650
Exercise:   7.1b
"""

import pygeohash
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as parq


def get_data_center_val(lat, long, westval, centralval, eastval):
    def get_center(val):
        for key, value in parts.items():
            if val == value:
                return key
        return "key doesn't exist"

    values = []
    locations = ['west','central','east']
    srcgeoval = pygeohash.encode(lat, long)
    distm_west = pygeohash.geohash_approximate_distance(srcgeoval, westval) / 1000
    values.append(distm_west)
    distm_central = pygeohash.geohash_approximate_distance(srcgeoval, centralval) / 1000
    values.append(distm_central)
    distm_east = pygeohash.geohash_approximate_distance(srcgeoval, eastval) / 1000
    values.append(distm_east)
    parts = dict(zip(locations, values))
    closestdist =  min(values, key=float)
    center = get_center(closestdist)
    return center

westval = pygeohash.encode(45.5945645, -121.1786823, precision=12)
print(westval)
centralval = pygeohash.encode(41.1544433, -96.0422378, precision=12)
print(centralval)
eastval = pygeohash.encode(39.08344, -77.6497145, precision=12)
print(eastval)

base_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment07/')
results_dir = base_dir.joinpath('results')
parquet_file = results_dir.joinpath('routes.parquet')
partitioned_parquet_file = results_dir.joinpath('geo')
csv_file = results_dir.joinpath('routes_parquet_datacenters.csv')
data_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment07/').joinpath('Data')

pq = pd.read_parquet(parquet_file, engine='fastparquet')

pq['key'] = pq['src_airport.iata']+pq['dst_airport.iata']+pq['airline.icao']
pq['lat'] = pq['src_airport.latitude']
pq['long'] = pq['src_airport.longitude']
pq['location'] = pq.apply(lambda x: get_data_center_val(x.lat, x.long, westval, centralval, eastval), axis=1)

# For troubleshooting
# pq.to_csv(csv_file, sep='\t')
table = pa.Table.from_pandas(pq)

parq.write_to_dataset(
    table,
    root_path=partitioned_parquet_file,
    partition_cols=['location'],
)

ptable = parq.read_table(partitioned_parquet_file)
print(ptable)
