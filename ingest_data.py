#!/usr/bin/env python
# coding: utf-8

import os
import sys
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import gzip

def determine_output_path(url):
    if url.endswith('.gz'):
        return 'output_data.csv.gz'
    elif url.endswith('.csv'):
        return 'output_data.csv'
    else:
        print("Error: Invalid file extension. Only .csv and .gz are supported.")
        sys.exit(1)  # Exit the script with a non-zero status to indicate an error
    
def download_and_process_file(url, chunksize=100000):
    
    output_path = determine_output_path(url)
    # Download the file
    try:
        os.system(f"wget {url} -O {output_path}")
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None
    
    # Determine file type and process accordingly
    if output_path.endswith('.gz'):
        # Handle gzip file
        gz = gzip.open(output_path, 'rt')
        df_iter = pd.read_csv(gz, iterator=True, chunksize=chunksize)
    elif output_path.endswith('.csv'):
        # Handle regular CSV file
        df_iter = pd.read_csv(output_path, iterator=True, chunksize=chunksize)
    else:
        print(f"Unsupported file format: {output_path}")
        return None

    return df_iter

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    chunksize = params.chunksize
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    
    df_iter = download_and_process_file(url, chunksize)
    df = next(df_iter)

    #df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    #df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    try:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')
    except Exception as e:
        print(f"Error inserting data into the database: {e}")

    while True: 

        try:
            t_start = time()
            df = next(df_iter)
            #df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            #df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    parser.add_argument('--chunksize', type=int, default=100000, help='Chunk size for processing CSV')


    args = parser.parse_args()

    main(args)