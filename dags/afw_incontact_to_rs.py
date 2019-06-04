# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 09:38:34 2019

@author: kanyad
"""
from airflowlib.config import config
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime,timedelta, date
import pandas as pd
import datetime as dt
import psycopg2
from airflow.models import Connection
from airflow.exceptions import AirflowException
from airflow.utils.db import provide_session
from airflow.hooks.S3_hook import S3Hook
import time
import os.path
import datetime as dtime
import psycopg2, boto3
import os,sys,inspect


year = str(dtime.date.today().year)

month = str(dtime.date.today().month)

day = str(dtime.date.today().day)
print(' year --', year, ' month -- ', month, ' day --',day)


ts = time.time()

current_timestamp = dt.datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H_%M')

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 


class sprt_in_contact:
    def __init__(self, bucket_name):
        #get redshift connection
        self.pg_hook = PostgresHook(postgres_conn_id=config.rs_conn_id)
        self.bucket_name = bucket_name
        self.rs_load_dag_config = Variable.get("aws_rs_config", deserialize_json=True)        
        self.aws_access_key_id = self.rs_load_dag_config["access_key_id"]
        self.aws_secret_access_key = self.rs_load_dag_config["secret_access_key"]
        self.aws_rs_arn = self.rs_load_dag_config["aws_arn_redshift_etl_role"]
        self.aws_region_name = self.rs_load_dag_config["region_name"]
        
        
    @staticmethod
    def get_curr_date():
        now = str(date.today())
        return now
    
    def truncate_table(self, table_name):
        con = self.pg_hook.get_conn()    
        query = 'truncate table '+table_name
        cursor = con.cursor()
        cursor.execute(query)
        con.commit()
        con.close()
    
    def load_inContact_data_to_redshift_full(self, type_of_data:str, table_name:str):
        
        self.truncate_table(table_name)                
        s3_file_path = 's3://{bucket_name}/inContact/'+type_of_data+'/'
        s3_file_path = s3_file_path.format(bucket_name = self.bucket_name)
        query = """copy {table_name} from '{s3_file_path}' 
                   iam_role  '{aws_rs_arn}'  region '{aws_region_name}' 
                   delimiter ',' 
                   IGNOREHEADER 1
                   EMPTYASNULL CSV NULL AS '\\0'
                   maxerror as 100 """.format(bucket_name = self.bucket_name, 
                                              table_name = table_name, 
                                              s3_file_path= s3_file_path, 
                                              aws_rs_arn = self.aws_rs_arn, 
                                              aws_region_name = self.aws_region_name  )

        con = self.pg_hook.get_conn()    
        cursor = con.cursor()
        cursor.execute(query)       
        con.commit()
        con.close()
            
    def load_inContact_data_to_redshift_inc(self, type_of_data:str, table_name:str):
        s3_file_name = type_of_data+'_data_'+self.get_curr_date()+'.csv'
        s3_file_path = 's3://{bucket_name}/inContact/'+type_of_data+'/'+s3_file_name
        query = """copy {table_name} from '{s3_file_path}' 
                   iam_role  '{aws_rs_arn}'  region '{aws_region_name}' 
                   delimiter ',' 
                   IGNOREHEADER 1
                   EMPTYASNULL CSV NULL AS '\\0'
                   maxerror as 100 """.format(bucket_name = self.bucket_name, 
                                              table_name = table_name, 
                                              s3_file_path= s3_file_path, 
                                              aws_rs_arn = self.aws_rs_arn, 
                                              aws_region_name = self.aws_region_name  )
        
        con = self.pg_hook.get_conn()    
        cursor = con.cursor()
        cursor.execute(query)       
        con.commit() 
        con.close()
        
