from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from MinioFileSensor import MinioFileSensor
from spark_job import import_csv_to_staging
from dq_check import staging_to_transactions

default_args = {
    'owner': 'sarthak',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
    'depend_on_past':True
}

with DAG(
    dag_id='data_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['data-engineering']
) as dag:
    
    def load_to_postgres():
        print("Running Spark job to parse file and load to postgres...")
        import_csv_to_staging()

    def dq_check_and_transformation():
        print("Transforming data and inserting into final table...")
        staging_to_transactions()

    t1 = MinioFileSensor(
        task_id='check_minio_file',
        bucket='daily-data',
        key='input.csv',
        poke_interval=60,
        timeout=3600
    )
    t2 = PythonOperator(task_id='load_to_postgres', python_callable=load_to_postgres)
    t3 = PythonOperator(task_id='transform_data', python_callable=dq_check_and_transformation)

    t1 >> t2 >> t3
