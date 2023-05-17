from airflow import DAG
from datetime import datetime 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import BranchPythonOperator


import pandas as pd
import sys
sys.path.append('/opt/airflow/includes/')
from emp_dim_insert_update import join_and_detect_new_or_changed_rows
import queries


def Branching_TS_fn(**kwargs):
    ids_to_update = kwargs['ti'].xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")
    if len(ids_to_update)>0:
        return 'update_task'
    else:
        return 'ins_task'

with DAG("S3_Connection_Dag", start_date= datetime(2023,5,13), schedule='@hourly' , catchup=False) as dag :
  
    sql_to_s3_task1 = SqlToS3Operator( task_id="sql_to_s3_task1", sql_conn_id='Project_Sql_connS3',query= 'select * from finance.emp_sal',
    s3_bucket='staging.emp.data',s3_key='Yousra_emp_details.csv',replace=True,aws_conn_id = 'AWS_Conn')
    
    sql_to_s3_task2 = SqlToS3Operator( task_id="sql_to_s3_task2", sql_conn_id='Project_Sql_connS3',query='select * from hr.emp_details',
    s3_bucket='staging.emp.data',s3_key='Yousra_emp_sal.csv',replace=True , aws_conn_id = 'AWS_Conn')
    

    #Extract_from_S3_Task = join_and_detect_new_or_changed_rows()
    Branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=Branching_TS_fn ) 
    
    insert_task = SnowflakeOperator(
        task_id="ins_task",
        sql=queries.INSERT_INTO_DWH_EMP_DIM("{{ti.xcom_pull(task_ids = 'join_and_detect_new_or_changed_rows', key='rows_to_insert')}}"),
        snowflake_conn_id="Snowflake_conn" )
        
        
        #sql = queries.INSERT_INTO_DWH_EMP_DIM("{{ti.xcom_pull(task_ids='join_and_detect_new_or_changed_rows', key='rows_to_insert')}}")

    
    update_task = SnowflakeOperator(
     task_id="update_task",
     sql=queries.UPDATE_DWH_EMP_DIM("{{ti.xcom_pull(task_ids ='join_and_detect_new_or_changed_rows',key='ids_to_update' )}}"),
     snowflake_conn_id="Snowflake_conn" )
     
     
    insert2_task = SnowflakeOperator(
        task_id="ins2_task",
        sql=queries.INSERT_INTO_DWH_EMP_DIM("{{ti.xcom_pull(task_ids = 'join_and_detect_new_or_changed_rows', key='rows_to_insert')}}"),
        snowflake_conn_id="Snowflake_conn" )
   

 
    [sql_to_s3_task1 , sql_to_s3_task2] >> join_and_detect_new_or_changed_rows()>> Branch_task
    Branch_task>> update_task >> insert2_task
    Branch_task>>insert_task
