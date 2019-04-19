import time
import logging
import datetime


class config:
    
    env = 'preview'
    aws_conn_id = 'aws_s3_sprtdev'
    
    # timestamp for the output file
    time_stamp = time.strftime("%Y%m%d_%H%M%S")

    conn_string = "dbname='preview_container_one' port='5432' user='postgres' password='' host='' "    
    bucket_name = 's3-data-lake'
    
    ####  Tenant = SOS ####  
    sos_tenant_id = 'tenant_69df535b6577361'
    sos_folder_name = 'sos'    
    sos_retry_count = 2
    sos_table_names = {'activity_log':'I', 'consumer':'F','consumer_credential':'F','session_note':'I','session':'I','consumer_subscription':'F',
               'rollup_selfservice_session_workflow':'I','workflow_version':'I','workflow':'I', 'user':'I','chat_message':'I', 'session_extension':'I'}
    
    sos_table_cdc = {'session_extension':'time_modified', 'activity_log':'time_inserted', 'chat_message':'time_inserted','session':'time_modified', 'rollup_selfservice_session_workflow':'time_modified',
                     'workflow_version':'time_modified', 'workflow':'time_modified', 'session_note':'time_created' , 'user':'time_modified'}
    
    sos_dag_name = 'sos_dag'
    sos_scheduled_interval = '@daily'
    sos_task_id_prefix = 'sos_'+env+'_'
    sos_delta_load_no_of_days = 1
