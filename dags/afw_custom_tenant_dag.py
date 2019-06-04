# -*- coding: utf-8 -*-
"""
Created on Tue May 14 09:04:45 2019

@author: kanyad
"""

from airflowlib.config import config
#from afw_custom_to_rs import sprt_custom_tenant
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime,timedelta, date
import datetime as dt
from airflow.hooks.S3_hook import S3Hook
import time
import os.path
import datetime as dtime
import urllib
import urllib.request
import os
import pandas as pd


import pathlib


year = str(dtime.date.today().year)

month = str(dtime.date.today().month)

day = str(dtime.date.today().day)
print(' year --', year, ' month -- ', month, ' day --',day)


ts = time.time()

current_timestamp = dt.datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H_%M')

tenant_params = Variable.get("custom_tenant_params", deserialize_json=True) 
       
############## Importing Config variables ###########################
tenant_name = tenant_params["tenant_name"]
folder_name = tenant_name
bucket_name = config.bucket_name
scheduled_interval = config.custom_scheduled_interval
dag_name = config.custom_dag_name.format(custom_tenant=tenant_name)
task_id_prefix = config.custom_task_id_prefix.format(custom_tenant=tenant_name)
aws_conn_id = config.aws_conn_id
delta_load_no_of_days = config.custom_delta_load_no_of_days
table_names =  tenant_params["table_names"] if len(tenant_params["table_names"]) >0 else config.custom_table_names 
app_postgres_conn_id =  config.app_postgres_conn_id
app_global_postgres_conn_id =  config.app_global_postgres_conn_id
schema_name = 'sprt_'+tenant_name+'_s3_dwh'

now = str(date.today())
print("Today in yyyy-mm-dd ::::", now)
start_date = str(date.today() - timedelta(delta_load_no_of_days))
end_date = now
print("start date ---->", start_date)
print("end date ---->", end_date)

def get_tenant_id(inp_tenant_name:str)->str:
    pg_hook = PostgresHook(postgres_conn_id=app_global_postgres_conn_id)
    con = pg_hook.get_conn()    
    cursor  = con.cursor()
    print(' Get tenant id ')
    tenant_get_query = "select id from tenant where alias = '{tenant_name}'".format(tenant_name=inp_tenant_name)
    #tenant_get_query = "select * from tenant "
    print('tenant_get_query --->', tenant_get_query)
    try:
        cursor.execute(tenant_get_query)
        rows = cursor.fetchone()
        print(rows[0])
        tenant_id = 'tenant_'+rows[0].replace('-','')
        print('tenant_id--->',tenant_id)
    except Exception as ERROR:
        print(' no tenant with the given tenant name ')
        return
    finally:
        cursor.close()
        con.close()
    
    return tenant_id

def update_app_load_stats(table_name, record_count, load_type, load_status):
    print('update load stats')
    pg_hook = PostgresHook(postgres_conn_id=config.app_afw_meta_db)
    con = pg_hook.get_conn()
    cursor = con.cursor()
    insert_sql = """insert into app_load_stats(dag_name, 
                                         tenant_name, 
                                         table_name,
                                         load_type, 
                                         record_count, 
                                         triggered_by, 
                                         load_status) values (
                                         '{dag_name}', 
                                         '{tenant_name}',
                                         '{table_name}',
                                         '{load_type}',
                                         {record_count},
                                         '{triggered_by}',
                                         {load_status}) """.format(dag_name = dag_name,
                                                                   tenant_name = tenant_name,
                                                                   table_name = table_name,
                                                                   load_type = load_type,
                                                                   load_status = load_status,
                                                                   record_count = record_count,
                                                                   triggered_by = config.owner_name)
    try:
        cursor.execute(insert_sql)
        con.commit()
    except Exception as ERROR:
        print('error inserting load stats into metadata table ', ERROR)
    finally:
        cursor.close()
        con.close()
    

def load_data_to_s3(table_name):   
    pg_hook = PostgresHook(postgres_conn_id=app_postgres_conn_id)
    con = pg_hook.get_conn()    
    cursor  = con.cursor()
    tenant_id = get_tenant_id(tenant_name)
    set_query = "SET search_path TO {tenant_id}".format(tenant_id = tenant_id)
    cursor.execute(set_query)
    
    if table_name == 'user':
        table_name = '"user"'
    
    query = "select * from "+ table_name 
    try:
        print('loading table ',table_name)
        cursor.execute(query)
        print('executing app get query')
        update_app_load_stats(table_name, cursor.rowcount, 'F', True)
        print('updated load stats')
        if cursor.rowcount> 0:
            column_names = [i[0] for i in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=column_names)
            table_name = table_name.replace('"','')
    #        if table_name == 'session_extension':
    #        
    #            df['descriptionofissue'] =  df['descriptionofissue'].str.replace('|', ':')
    #            df['descriptionofissue'] =  df['descriptionofissue'].str.replace('\n', '')
    #            df['descriptionofissue'] =  df['descriptionofissue'].replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True)
    #            df['descriptionofissue'] =  df['descriptionofissue'].str.encode('utf-8')
    #            df['descriptionofissue'] =  df['descriptionofissue'].str.decode('utf-8')
    #            df['crnumber'] =  df['crnumber'].str.replace('|', ':')
    #            df['customername'] =  df['customername'].str.replace('|', ':')
    #        
            if table_name == 'activity_log':
                df['session_id'] = df['session_id'].apply(lambda x: int(x) if x == x else '')
                df['consumer_id'] = df['consumer_id'].apply(lambda x: int(x) if x == x else '')
                df['user_id'] = df['user_id'].apply(lambda x: int(x) if x == x else '')
                df['session_device_id'] = df['session_device_id'].apply(lambda x: int(x) if x == x else '')
                df['session_workflow_id'] = df['session_workflow_id'].apply(lambda x: int(x) if x == x else '')
                df['session_workflow_step_id'] = df['session_workflow_step_id'].apply(lambda x: int(x) if x == x else '')
            
            if table_name == 'chat_message':
                df['consumer_id'] = df['consumer_id'].apply(lambda x: int(x) if x == x else '')
                df['user_id'] = df['user_id'].apply(lambda x: int(x) if x == x else '')
                
            output_filename = "/usr/local/airflow/"+table_name+".csv"
        
            df.to_csv(output_filename, sep='|', index=False)
    except Exception as ERROR:
        update_app_load_stats(table_name, cursor.rowcount, 'F', False)
        print('error executing query in load_data_to_s3')
    finally:
        cursor.close()
        con.close()


def upload_file_to_S3(bucket_name, table_name):
    
    file_path_name = '/usr/local/airflow/'+table_name+'.csv'
    out_filename = folder_name+'/'+table_name+'/'+table_name+'.csv'  
    
    if os.path.isfile(file_path_name):
        s3 = S3Hook(aws_conn_id=aws_conn_id)
        s3.load_file(file_path_name, out_filename, bucket_name=bucket_name,replace=True)
    else:
        print("No file exists ::::",file_path_name)

def create_schema_in_rs(schema_name):
    pg_hook = PostgresHook(postgres_conn_id=config.rs_conn_id)
    rs_load_dag_config = Variable.get("aws_rs_config", deserialize_json=True)       
    aws_rs_spect_arn = rs_load_dag_config["aws_rs_spect_arn"]
    aws_rs_spect_db = rs_load_dag_config["aws_rs_spect_db"]
    aws_region_name = rs_load_dag_config["region_name"]
    con = pg_hook.get_conn()    
    
    query = "create external schema if not exists {schema_name} from data catalog database '{db}' iam_role '{aws_iam_role}' region '{region_name}' ".format(schema_name = schema_name,
                                    db = aws_rs_spect_db, 
                                    aws_iam_role = aws_rs_spect_arn,
                                    region_name = aws_region_name)
    
    print('schema creation query ---', query)
    cursor = con.cursor()
    cursor.execute(query)       
    cursor.close()
    con.close()
        
def create_table_in_rs(bucket_name, tenant_name, schema_name, table_name):
    
    set_query =' SET AUTOCOMMIT = ON '
    
    current_dir = pathlib.Path(__file__).parent

    print('current_dir---',current_dir)
    
    file_name = str(current_dir)+'/db/ddl/'+table_name+'.sql'
    with open(file_name,'r') as f:
        ddl_query = f.read().replace("\n",'')
    
    print('ddl_query ---',ddl_query)
    pg_hook = PostgresHook(postgres_conn_id=config.rs_conn_id)
    con = pg_hook.get_conn()    
    location = 's3://'+bucket_name+'/'+tenant_name+'/'+table_name+'/'
    ddl_query = ddl_query.format(schema_name=schema_name,location=location )
    print('ddl_query ---',ddl_query)
    cursor = con.cursor()
    cursor.execute(set_query)
    cursor.execute(ddl_query)       
    
    con.commit()
    cursor.close()
    con.close()
    


default_args = {
    'owner': config.owner_name,
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 17),
    'email': config.email_dl,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': config.custom_retry_count,
    'retry_delay': timedelta(minutes=5),    
    'schedule_interval': '@daily'
}

dag = DAG(dag_name, default_args=default_args, catchup=False)


run_this_first = DummyOperator(
    task_id='run_this_first',
    dag=dag,
)

check_if_tenant_exists_task = PythonOperator(
    task_id='tenant_exists_task',
    python_callable= get_tenant_id,
    op_kwargs={
                'inp_tenant_name': tenant_name
                },
    dag=dag,
)
    

create_external_schema_for_tenant = PythonOperator(
                                task_id='create_external_schema_rs_task',
                                python_callable=create_schema_in_rs,
                                op_kwargs={
                                        'schema_name': schema_name
                                        },
                                        dag=dag
                                        )

print_curr_date = BashOperator(
task_id='printing_date',
bash_command='date',
dag=dag)    


rm_temp_files = BashOperator(
                        task_id='delete_temp_file',
                        bash_command='rm -f /usr/local/airflow/*.csv ',
                        dag=dag)    

run_this_first >> check_if_tenant_exists_task >> create_external_schema_for_tenant >> print_curr_date

#check_if_tenant_exists_task.set_downstream(branch_a)

for table_name in  table_names:    
        
    task_id = task_id_prefix +table_name+ '_to_s3'    
    opr_load_data_to_s3 = PythonOperator(task_id= task_id, python_callable=load_data_to_s3,
                                                     op_kwargs={
                                                            'table_name': table_name
                                                        },
                                                     dag=dag)
    upload_to_S3_task = PythonOperator(
                            task_id='upload_file_to_S3_'+table_name,
                            python_callable=upload_file_to_S3,
                            op_kwargs={
                                    'table_name': table_name,
                                    'bucket_name': bucket_name
                                    },
                                    dag=dag
                                    )
#    create_table_in_rs_task = PythonOperator(
#                            task_id='create_'+table_name+'_in_rs',
#                            python_callable=create_table_in_rs,
#                            op_kwargs={                                    
#                                    'bucket_name': bucket_name,
#                                    'tenant_name':tenant_name,
#                                    'table_name': table_name,
#                                    'schema_name':schema_name
#                                    },
#                                    dag=dag
#                                    )
  
    
    print_curr_date  >> opr_load_data_to_s3 >> upload_to_S3_task >> rm_temp_files 
            