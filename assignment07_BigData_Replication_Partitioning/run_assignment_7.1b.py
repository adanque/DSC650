"""
Author:     Alan Danque
Date:       20210101
Class:      DSC 650
Exercise:   7.1b
"""

import hashlib
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as parq

def hash_key(key):
    m = hashlib.sha256()
    m.update(str(key).encode('utf-8'))
    return m.hexdigest()

base_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment07/')
results_dir = base_dir.joinpath('results')
parquet_file = results_dir.joinpath('routes.parquet')
partitioned_parquet_file = results_dir.joinpath('hash')
csv_file = results_dir.joinpath('routes_parquet.csv')
data_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment07/').joinpath('Data')

pq = pd.read_parquet(parquet_file, engine='fastparquet')

pq['key'] = pq['src_airport.iata']+pq['dst_airport.iata']+pq['airline.icao']
pq['hashed'] = pq.apply(lambda x: hash_key(x.key), axis=1)
pq['hash_key'] = pq['hashed'].str[:1]

# For troubleshooting
#pq.to_csv(csv_file, sep='\t')
table = pa.Table.from_pandas(pq)

parq.write_to_dataset(
    table,
    root_path=partitioned_parquet_file,
    partition_cols=['hash_key'],
)

ptable = parq.read_table(partitioned_parquet_file)
print(ptable)
