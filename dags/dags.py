from airflow.models import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'yuditya',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG('pipeline_workflow', start_date=datetime(2022, 10, 14), max_active_runs=3, schedule_interval='@daily', default_args=default_args)

t1 = BashOperator(
    task_id='insert_data_sources',
    bash_command='python3 /mnt/e/dataengineer/project/finalproject/data/appInsert.py',
    dag=dag)

t2 = BashOperator(
    task_id='restructure_dwh',
    bash_command='python3 /mnt/e/dataengineer/project/finalproject/data/appLoad.py',
    dag=dag)

t3 = BashOperator(
    task_id='spark_execute',
    bash_command='python3 /mnt/e/dataengineer/project/finalproject/sparkApp.py',
    dag=dag)

t4 = BashOperator(
    task_id='mapreduce_execute',
    bash_command='/mnt/e/dataengineer/project/finalproject/mapreduce/./run.sh',
    dag=dag)

t5 = BashOperator(
    task_id='upload_aws_s3',
    bash_command='python3 /mnt/e/dataengineer/project/finalproject/data/processed_data/s3upload.py',
    dag=dag)

t1 >> t2 >> t3 >> t5 << t4

    if __name__ =='__main__':
        dag.cli()