from airflowlib.config import config
from airflow import DAG
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


year = str(dtime.date.today().year)

month = str(dtime.date.today().month)

day = str(dtime.date.today().day)
print(' year --', year, ' month -- ', month, ' day --',day)


ts = time.time()

current_timestamp = dt.datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H_%M')

############## Importing Config variables ###########################
folder_name = config.sos_folder_name
conn_string = config.conn_string
tenant_id = config.sos_tenant_id
bucket_name = config.bucket_name
scheduled_interval = config.sos_scheduled_interval
dag_name = 'sos_dag_v1.1'
task_id_prefix = config.sos_task_id_prefix
cdc_table_col = config.sos_table_cdc
aws_conn_id = config.aws_conn_id
delta_load_no_of_days = config.sos_delta_load_no_of_days

now = str(date.today())
print("Today in yyyy-mm-dd ::::", now)
start_date = str(date.today() - timedelta(delta_load_no_of_days))
end_date = now
print("start date ---->", start_date)
print("end date ---->", end_date)

def load_data_to_s3(table_name, full_or_inc, cdc_col_name=''):    
   
    con = psycopg2.connect(conn_string)
    
    cursor = con.cursor()            
    
    set_query = "SET search_path TO "+tenant_id
    cursor.execute(set_query)
    
    if table_name == 'user':
        table_name = '"user"'

    if full_or_inc == 'I':
        cdc_col_filter = " where "+ cdc_col_name+"::date >= '"+start_date + "' and "+ cdc_col_name + "::date < '"+end_date+"'"        
        query = "select * from "+ table_name + cdc_col_filter 
    elif full_or_inc == 'F':
        query = "select * from "+ table_name 
    cursor.execute(query)
    if cursor.rowcount> 0:
        column_names = [i[0] for i in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=column_names)
        table_name = table_name.replace('"','')
        
        output_filename = "/usr/local/airflow/"+table_name+".csv"
    
        df.to_csv(output_filename, sep='|', index=False)
    con.close()


def upload_file_to_S3(bucket_name, table_name, replace):
    
    file_path_name = '/usr/local/airflow/'+table_name+'.csv'
    
    if replace :
        out_filename = folder_name+'/'+table_name+'/'+table_name+'.csv'        
    else:        
        out_filename = folder_name+'/'+table_name+'/'+table_name+'_'+current_timestamp+'.csv'
                
    
    if os.path.isfile(file_path_name):
        s3 = S3Hook(aws_conn_id=aws_conn_id)
        s3.load_file(file_path_name, out_filename, bucket_name=bucket_name,replace=replace)
    else:
        print("No file exists ::::",file_path_name)


default_args = {
    'owner': config.owner_name,
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 5),
    'email': config.email_dl,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': config.sos_retry_count,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_name, default_args=default_args, schedule_interval= scheduled_interval)

print_curr_date = BashOperator(
task_id='printing_date',
bash_command='date',
dag=dag)    


rm_temp_files = BashOperator(
task_id='delete_temp_file',
bash_command='rm -f /usr/local/airflow/*.csv ',
dag=dag)    


table_names = config.sos_table_names
 
for table_name, full_or_inc in  table_names.items():
        
    print('table_name ::::',table_name,'---->',full_or_inc)
    if full_or_inc == 'F' :
        replace_or_not = True        
    else:
        replace_or_not = False
        cdc_col_name = cdc_table_col[table_name]
    
    task_id = task_id_prefix +table_name+ '_to_s3'
    
        
    opr_load_sos_activity_log_to_s3 = PythonOperator(task_id= task_id, python_callable=load_data_to_s3,
                                                     op_kwargs={
                                                            'table_name': table_name,
                                                            'full_or_inc' : full_or_inc,
                                                            'cdc_col_name': cdc_col_name
                                                        },
                                                     dag=dag)


    
    
    upload_to_S3_task = PythonOperator(
                task_id='upload_file_to_S3_'+table_name,
                python_callable=upload_file_to_S3,
        op_kwargs={
            'table_name': table_name,
            'bucket_name': bucket_name,
            'replace' : replace_or_not
        },
        dag=dag
        )


    print_curr_date >> opr_load_sos_activity_log_to_s3 >> upload_to_S3_task >> rm_temp_files



    