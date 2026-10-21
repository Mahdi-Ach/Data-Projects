from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import datetime, timedelta
with DAG(
    'lakehouse_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval=timedelta(seconds=30),
    catchup=False,
) as dag:

    produce = BashOperator(task_id='produce', bash_command='python /opt/airflow/producer.py')
    consume = BashOperator(task_id='consume', bash_command='python /opt/airflow/consumer_to_bronze.py')
    silver = BashOperator(task_id='silver', bash_command='python /opt/airflow/silver_to_minio.py')
    load = BashOperator(task_id='load', bash_command='python /opt/airflow/load_postgres.py')
    run_dbt = BashOperator(task_id='dbt', bash_command='cd /opt/airflow/gold_project && dbt run --profiles-dir .')

    produce >> consume >> silver >> load >> run_dbt