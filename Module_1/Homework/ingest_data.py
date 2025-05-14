import os
import pandas as pd
from pathlib import Path
import argparse
from sqlalchemy import create_engine
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    database_name = params.database_name
    port = params.port
    url = params.url
    table_name = params.table_name

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz' 
    else:
        csv_name = 'output.csv' 

    # download the csv file
    # Idenpotency -- Don't download file which alfeady exists
    if not Path(csv_name).exists():
        os.system(f'wget {url} -O {csv_name}')        
    else:
        print(f'{csv_name} already downloaded. Skipping')
        
    # DB Connection  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')

    #read data
    df_inter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_inter)
    # convert timestamp datatype
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    #transport the headers i.e form the table schema
    df.head(n=0).to_sql(table_name,con=engine,if_exists='replace')
    df.to_sql(table_name,con=engine,if_exists='append')

    while True:
        t_start = time()

        df = next(df_inter)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

        df.to_sql(table_name,con=engine,if_exists='append')
        
        t_end = time()

        print(f'Inserted another chunck of data, this took {t_end - t_start:.3f} seconds')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Function which ingests data into Postgresql')
    parser.add_argument('--user',help='user of the db')    
    parser.add_argument('--password',help='password for the db')    
    parser.add_argument('--host',help='host for the db')   
    parser.add_argument('--port',help='port')   
    parser.add_argument('--table_name',help='name of the table')   
    parser.add_argument('--database_name',help='name of the db' )  
    parser.add_argument('--url',help='Url which holds the data that we need to ingest')                

    args = parser.parse_args()
    main(args)