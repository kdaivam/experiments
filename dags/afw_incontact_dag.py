# -*- coding: utf-8 -*-
"""
Created on Tue May 14 09:04:45 2019

@author: kanyad
"""

from airflowlib.config import config
from afw_incontact_to_rs import sprt_in_contact
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
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


year = str(dtime.date.today().year)

month = str(dtime.date.today().month)

day = str(dtime.date.today().day)
print(' year --', year, ' month -- ', month, ' day --',day)


ts = time.time()

current_timestamp = dt.datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H_%M')

date_params = Variable.get("incontact_date_params", deserialize_json=True) 
       
############## Importing Config variables ###########################
folder_name = config.inContact_folder_name
bucket_name = config.bucket_name
scheduled_interval = config.inContact_scheduled_interval
dag_name = 'inContact_dag_daily_load'
task_id_prefix = config.inContact_task_id_prefix
aws_conn_id = config.aws_conn_id
delta_load_no_of_days = config.inContact_delta_load_no_of_days

now = str(date.today())
print("Today in yyyy-mm-dd ::::", now)
start_date = str(date.today() - timedelta(delta_load_no_of_days))
end_date = now
print("start date ---->", start_date)
print("end date ---->", end_date)


def get_curr_date():
    now = str(date.today())
    return now

def get_curr_date_in_mdy():
    now = dtime.datetime.today()
    return str(now.year), str(now.month), str(now.day)

def get_delta_date_in_mdy(delta_days):
    d = dtime.datetime.today() - timedelta(days=delta_days)
    return str(d.year), str(d.month), str(d.day)

def get_from_and_to_dates():
    pre_year, pre_month, pre_day = get_delta_date_in_mdy(date_params["delta_days"])
    cur_year, cur_month, cur_day = get_curr_date_in_mdy()
    date_from = pre_month+'%2f'+pre_day+'%2f'+pre_year
    date_to = cur_month+'%2f'+cur_day+'%2f'+cur_year
    
    return date_from, date_to
    
def get_call_data_api():    
    date_from, date_to = get_from_and_to_dates()
    url = "https://home-c29.incontact.com/ReportService/DataDownloadHandler.ashx?CDST=DvNTMW4T0RlBplXtk8CsRtMhnckMqYcZKfUAKTaT9pmSsMWRuk%2flzYXlwh65HjY5sFrk4O%2bJdzyhnVasKhIYWPsqOX%2bVA45v8MHjV9NpAnX7xDcLT1d14GG8hXpcBzZNUEt7bUsOpmckNkY7TkUgoQbrwCU6wRv7yWyEl03%2f5XNVVovoiBNoiVEthEU04YT0540bNY8P3PY7FiiE5Q%3d%3d&DateFrom={}+8%3a00%3a00+AM&DateTo={}+7%3a59%3a59+AM&Format=CSV&IncludeHeaders=True&AppendDate=False".format(date_from, date_to)
    return url

def get_agent_skill_data_api():
    date_from, date_to = get_from_and_to_dates()
    url = "https://home-c29.incontact.com/ReportService/DataDownloadHandler.ashx?CDST=DvNTMW4T0RlBplXtk8CsRtMhnckMqYcZKfUAKTaT9pkBOQLbHNT53ztghfe7ryfmUfVxx4%2fyABPRoxmqKoL6U23r3SQHopz17jKpYMdl0kxU2GuETvAnYb8bUsHtF3Rj%2bzbCozUcybtco2Oe3Cn%2fmA7pNB%2f%2bEwV4qGirp9UJTLZpRDd7lkSG3lJA0lnlUFMyiJ1FK6qBDB%2fb8mDJ8A%3d%3d&DateFrom={}+8%3a00%3a00+AM&DateTo={}+7%3a59%3a59+AM&Format=CSV&IncludeHeaders=True&AppendDate=False".format(date_from, date_to)
    return url

def get_skill_data_api():
    date_from, date_to = get_from_and_to_dates()
    url = "https://home-c29.incontact.com/ReportService/DataDownloadHandler.ashx?CDST=DvNTMW4T0RlBplXtk8CsRtMhnckMqYcZKfUAKTaT9pnwD377ouM9cmh2PlGJcwejj18OpI9I3Z5a%2bCfr2XVHi%2fhSnFtMrUpkKfikQR5%2f%2fAoekKAUsPKMdimDi9tOIzO%2b%2bt97IQ3AnWv42iVI1uJVmCVdQ2S6OjUxCji0GQkK6KFIDMGjcg4WOGAUmWS7bo%2ffnO%2bU8szio%2fR3XVs1iA%3d%3d&DateFrom={}+8%3a00%3a00+AM&DateTo={}+7%3a59%3a59+AM&Format=CSV&IncludeHeaders=True&AppendDate=False".format(date_from, date_to)
    return url

def get_contact_to_ticket_data_api():
    date_from, date_to = get_from_and_to_dates()
    url = "https://home-c29.incontact.com/ReportService/DataDownloadHandler.ashx?CDST=DvNTMW4T0RlBplXtk8CsRtMhnckMqYcZKfUAKTaT9pn0J5PVMF0KE5Z3G1NhlGXwxwMDm9KgLIDnxZb7uR53igGHCHeJ4quke4DmkgdNMQ3poDCcZ7SLDPP3wi%2bBRzqPrVaYjDRJRoYr5FHqC7DchHk2zRG3je9CPvGX0jQpd0gFRrSQ9qSZtZFxlxW%2bUJnau8TlNvEooiCMN8xBpw%3d%3d&DateFrom={}+8%3a00%3a00+AM&DateTo={}+7%3a59%3a59+AM&Format=CSV&IncludeHeaders=True&AppendDate=False".format(date_from, date_to)
    return url
    
def write_to_file(file_name:str, u):
    filename = '/usr/local/airflow/'+file_name
    with open(filename,'wb') as output:
        output.write(u.read())    
        
def get_file_name(table_name:str, full_or_inc:str) -> str:
    if full_or_inc == 'I':
        file_name = table_name+'_'+get_curr_date()+'.csv'
    else:
        file_name = table_name+'_full'+'.csv'
    
    return file_name

def load_data_to_s3(table_name, full_or_inc):
    file_name = get_file_name(table_name, full_or_inc)
    if table_name == 'call':
        api_url = get_call_data_api()
    elif table_name == 'skill':
        api_url = get_skill_data_api()
    elif table_name == 'contact_to_ticket':
        api_url = get_contact_to_ticket_data_api()
    elif table_name == 'agent_skill':
        api_url = get_agent_skill_data_api()
    
    u = urllib.request.urlopen(api_url)
    write_to_file(file_name, u)


def upload_file_to_S3(table_name:str, bucket_name:str, full_or_inc:str):
        
    file_name = get_file_name(table_name, full_or_inc)    
    file_path_name = '/usr/local/airflow/'+file_name
    
    output_file_name_to_s3 = folder_name+'/'+table_name+'/'+file_name
        
    if os.path.isfile(file_path_name):
        s3 = S3Hook(aws_conn_id=aws_conn_id)
        s3.load_file(file_path_name, output_file_name_to_s3, bucket_name=bucket_name,replace=True)
    else:
        print("No file exists ::::",file_path_name)

def copy_to_rs(table_name:str, bucket_name:str, full_or_inc:str):
    load_data_sprt_in_contact = sprt_in_contact(bucket_name)
    inContact_rs_tables = config.inContact_rs_tables
    
    for k,v in inContact_rs_tables.items():
        if full_or_inc == 'F':
            load_data_sprt_in_contact.load_inContact_data_to_redshift_full(k, v)  
        else:
            load_data_sprt_in_contact.load_inContact_data_to_redshift_inc(k, v)  
    

default_args = {
    'owner': config.owner_name,
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 14),
    'email': config.email_dl,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': config.inContact_retry_count,
    'retry_delay': timedelta(minutes=5),    
    'schedule_interval': '@daily'
}

dag = DAG(dag_name, default_args=default_args, catchup=False)

print_curr_date = BashOperator(
task_id='printing_date',
bash_command='date',
dag=dag)    

rm_temp_files = BashOperator(
                        task_id='delete_temp_file',
                        bash_command='rm -f /usr/local/airflow/*.csv ',
                        dag=dag)    


table_names = config.inContact_table_names

for table_name, full_or_inc in  table_names.items():    
        
    task_id = task_id_prefix +table_name+ '_to_s3'    
    opr_load_data_to_s3 = PythonOperator(task_id= task_id, python_callable=load_data_to_s3,
                                                     op_kwargs={
                                                            'table_name': table_name,
                                                            'full_or_inc' : full_or_inc
                                                        },
                                                     dag=dag)
    upload_to_S3_task = PythonOperator(
                    task_id='upload_file_to_S3_'+table_name,
                    python_callable=upload_file_to_S3, 
                    op_kwargs={
                            'table_name': table_name,
                            'bucket_name': bucket_name,
                            'full_or_inc' : full_or_inc
                            },
                            dag=dag
                            )    
    
    if table_name != 'skill' and table_name != 'agent_skill' and table_name != 'contact_to_ticket':
        print_curr_date  >> opr_load_data_to_s3 >> upload_to_S3_task >> rm_temp_files
    else:
        copy_to_rs_task =  PythonOperator(
                    task_id='copy_to_rs_'+table_name,
                    python_callable = copy_to_rs, 
                    op_kwargs={
                            'table_name': table_name,
                            'bucket_name': bucket_name,
                            'full_or_inc': full_or_inc
                            },
                            dag=dag
                            )    
    
        print_curr_date  >> opr_load_data_to_s3 >> upload_to_S3_task >> copy_to_rs_task >> rm_temp_files
            