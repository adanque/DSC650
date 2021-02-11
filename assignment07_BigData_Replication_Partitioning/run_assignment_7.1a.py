"""
Author:     Alan Danque
Date:       20210101
Class:      DSC 650
Exercise:   7.1a
"""

import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as parq

def get_key(val):
    for key, value in parts.items():
        #if val == str(value):
        if val in value:
            return key
    return "key doesn't exist"

partitions = (
        ('A', 'A'), ('B', 'B'), ('C', 'D'), ('E', 'F'),
        ('G', 'H'), ('I', 'J'), ('K', 'L'), ('M', 'M'),
        ('N', 'N'), ('O', 'P'), ('Q', 'R'), ('S', 'T'),
        ('U', 'U'), ('V', 'V'), ('W', 'X'), ('Y', 'Z')
    )
partitions_val_keys = (
        'A', 'B', 'C-D', 'E-F',
        'G-H', 'I-J', 'K-L', 'M',
        'N', 'O-P', 'Q-R', 'S-T',
        'U', 'V', 'W-X', 'Y-Z'
    )
parts = dict(zip(partitions_val_keys, partitions))
print(parts)

base_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment07/')
results_dir = base_dir.joinpath('results')
parquet_file = results_dir.joinpath('routes.parquet')
partitioned_parquet_file = results_dir.joinpath('kv')
csv_file = results_dir.joinpath('routes_parquet.csv')
data_dir = Path('C:/Users/aland/class/DSC650/dsc650/dsc650/assignments/assignment07/').joinpath('Data')

pq = pd.read_parquet(parquet_file, engine='fastparquet')
#pq = pd.read_parquet(parquet_file, engine='pyarrow')
print(list(pq.columns.values))

#pq['key'] = pq['src_airport.iata']+pq['dst_airport.iata']+pq['airline.icao']
pq['key'] = pq['src_airport.iata']+pq['dst_airport.iata']+pq['airline.icao']
pq['partition_value'] = pq['key'].str[:1]
pq['kv_key'] = pq.apply(lambda x: get_key(x.partition_value), axis=1)

# Remove bad keys
pq = pq[pq.kv_key != "key doesn't exist"]

# For troubleshooting
# pq.to_csv(csv_file, sep='\t')

table = pa.Table.from_pandas(pq)

parq.write_to_dataset(
    table,
    root_path=partitioned_parquet_file,
    partition_cols=['kv_key'],
)


ptable = parq.read_table(partitioned_parquet_file)
print(ptable)
