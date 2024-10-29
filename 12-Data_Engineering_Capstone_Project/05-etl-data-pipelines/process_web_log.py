from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt

default_args = {
    'owner': 'user',
    'start_date': dt.datetime(2024,10,28),
    'email': ['user@gmail.com'],
}

# Define the DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='Softcart access log ETL pipeline',
    schedule_interval=dt.timedelta(days=1),
)

# Use BashOperator to cut all IP addresses from the input
extract_data = BashOperator(
    task_id='extract_data',
    bash_command='cut -f1 -d" " $AIRFLOW_HOME/dags/capstone/accesslog.txt > $AIRFLOW_HOME/dags/capstone/extracted_data.txt',
    dag=dag,
)

# Filter out all the occurrences of IP address “198.46.149.143” from extracted_data.tx
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='grep -vw "198.46.149.143" $AIRFLOW_HOME/dags/capstone/extracted_data.txt > $AIRFLOW_HOME/dags/capstone/transformed_data.txt',
    dag=dag,
)

# Load the data to weblog.tar
load_data = BashOperator(
    task_id='load_data',
    bash_command='tar -zcvf $AIRFLOW_HOME/dags/capstone/weblog.tar $AIRFLOW_HOME/dags/capstone/transformed_data.txt'
)


# Task pipeline
extract_data >> transform_data >> load_data