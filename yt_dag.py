from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import yt_etl as yt

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@monthly'
}

dag = DAG(
    'youtube_dag',
    default_args=default_args,
    description='Youtube DAG with ETL process',
)


step1 = PythonOperator(
    task_id='get_source_data',
    python_callable=yt.get_data,
    dag=dag, 
)

step2 = PythonOperator(
    task_id='transform_source_data',
    python_callable=yt.transform_data,
    #provide_context=True,
    op_args=[yt.f1],
    dag=dag, 
)

step3 = PythonOperator(
    task_id='load_transformed_data',
    python_callable=yt.load_data,
    #provide_context=True,
    op_args=[yt.f2],  
    dag=dag, 
)

step4 = PythonOperator(
    task_id='view_as_structured_data',
    python_callable=yt.structure_data,
    dag=dag, 
)


step1>>step2>>step3>>step4
