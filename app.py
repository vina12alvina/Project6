import os
import connection
import sqlparse
import pandas as pd
from hdfs import InsecureClient
import logging

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    print('[INFO] Service ETL is Starting ...')
    
    try:
        # connection data source
        conf = connection.config('marketplace_prod')
        conn, engine = connection.psql_conn(conf, 'DataSource')
        cursor = conn.cursor()
        logging.info('Success connect PostgreSQL DataSource')

        # connection dwh
        conf_dwh = connection.config('dwh')
        conn_dwh, engine_dwh = connection.psql_conn(conf_dwh, 'DataWarehouse')
        cursor_dwh = conn_dwh.cursor()
        logging.info('Success connect PostgreSQL DataWarehouse')

        # connection hadoop
        conf_hadoop = connection.config('hadoop')
        logging.info(f'Hadoop config: {conf_hadoop}')
        
        # Pastikan 'user' tersedia dalam konfigurasi Hadoop
        if 'user' not in conf_hadoop:
            raise KeyError("Key 'user' not found in Hadoop configuration")

        client = InsecureClient(conf_hadoop['url'], user=conf_hadoop['user'])
        logging.info('Success connect to Hadoop')

        # get query string
        path_query = os.getcwd()+'/query/'
        with open(path_query+'query.sql', 'r') as file:
            query = sqlparse.format(file.read(), strip_comments=True).strip()

        # Jika tabel berada dalam skema tertentu, tambahkan skema dalam query
        # Misal, skema public: 
        # query = "SELECT ... FROM public.tb_orders a LEFT JOIN ..."

        # get schema dwh design
        path_dwh_design = os.getcwd()+'/query/'
        with open(path_dwh_design+'dwh_design.sql', 'r') as file:
            dwh_design = sqlparse.format(file.read(), strip_comments=True).strip()

        # get data
        print('[INFO] Service ETL is Running ...')
        df = pd.read_sql(query, engine)

        # create schema dwh (jika diperlukan)
        # cursor_dwh.execute(dwh_design)
        # conn_dwh.commit()

        # ingest data to dwh
        # df.to_sql('dim_orders', engine_dwh, if_exists='append', index=False)
        
        # ingest data to hadoop
        with client.write(
            'digitalskola/project/dim_orders_vina.csv', 
            encoding='utf-8'
            ) as writer:
                df.to_csv(writer, index=False)  

        print('[INFO] Service ETL is Success ...')
    except Exception as e:
        print('[INFO] Service ETL is Failed ...')
        
