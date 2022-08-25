import pandas as pd
from glob import glob
import sqlalchemy
import os

# path of folder
path = r'/Users/daniamohamed/Desktop/land-reg-test'

# iterate through all the files
for filename in os.listdir(path):
    if filename.endswith('.csv'):
        df = pd.read_csv(filename)
        # add column type based on the dwelling count
        df['type'] = ['SDU' if x==4 or x<4 else 'IOD' if x == 13 or 13 > x > 4 else 'WAYLEAVE' for x in df['dwelling_count']]
        cols = df.columns.to_list()
        # insert column after dwelling count
        cols = cols[:16] + cols[-1:] + cols[16:-1]
        df = df[cols]
        # writing into file
        df.to_csv(filename, index=False)

# finds all the csv files to merge into one database
data_files = sorted(glob('*.csv'))
merge_data = pd.concat(pd.read_csv(datafile,keep_default_na=False) for datafile in data_files)
merge_data.to_csv('final.csv', index=False)

# read data into dataframe and create engine for database import 
df = pd.read_csv('final.csv', header=0, low_memory=False)
engine = sqlalchemy.create_engine("sqlite:///land_registry.db")
# write data to SQL
df.to_sql('land_registry', engine, if_exists='replace', index=False, method=None, chunksize=100000)