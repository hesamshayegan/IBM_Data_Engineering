from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# DAG arguments
default_args = {
    'owner': 'dummy_name',
    'start_date': days_ago(0),
    'email': ['your_email_here'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define first task
unzip_data = BashOperator(
    task_id='unzip',
    bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
    dag=dag,
)

# define the second task
extract_data_from_csv = BashOperator(
    task_id='extract-from-csv',
    bash_command='cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv-data.csv',
    dag=dag,
)

# define the third task
extract_data_from_tsv = BashOperator(
    task_id='extract-from-tsv',
    bash_command='cut -d$"\t" -f5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv-data.csv',
    dag=dag,
)

# define the forth task
extract_data_from_fixed_width = BashOperator(
    task_id='extract-from-txt',
    bash_command='tr -s " " < /home/project/airflow/dags/finalassignment/payment-data.txt | cut -d" " -f11,12 \
> /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)


# define the fifth task
consolidate_data=BashOperator(
    task_id="consolidate",
   bash_command='paste /home/project/airflow/dags/finalassignment/csv-data.csv \
                /home/project/airflow/dags/finalassignment/tsv-data.csv \
                /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag
)


# define the sixth task
transform_data=BashOperator(
    task_id="transform",
    bash_command='tr [a-z] [A-Z] < /home/project/airflow/dags/finalassignment/extracted_data.csv \
                  > /home/project/airflow/dags/finalassignment/transformed_data.csv',
    dag=dag
)




# task pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data