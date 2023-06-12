from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.data_fetcher_bdr_13 import fetch_data_from_bouches_du_rhone
from lib.data_fetcher_nord_59 import fetch_data_from_nord
from lib.raw_bdr_13 import convert_raw_to_formatted_bdr
from lib.raw_nord_59 import convert_raw_to_formatted_nord
from lib.combine_data import combine_data
from lib.combine_data import file1, file2, output_file
#from lib.index_elastic import index_to_elastic

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'project_dag',
    default_args=default_args,
    description='A first DAG',
    schedule_interval='0 0 * * *',  # Exécution quotidienne à minuit
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    def task_bouches_du_rhone_source_to_raw():
        fetch_data_from_bouches_du_rhone()

    def task_nord_source_to_raw():
        fetch_data_from_nord()

    def task_bouches_du_rhone_raw_to_formatted():
        convert_raw_to_formatted_bdr("bouches_du_rhone.json", "20230611")

    def task_nord_raw_to_formatted():
        convert_raw_to_formatted_nord("nord.json", "20230611")

    def task_produce_usage():
        combine_data(file1, file2, output_file)

    def task_index_to_elastic():
        print("index")
        """""
        #index_to_elastic()
        """

    t1 = PythonOperator(
        task_id='bouches_du_rhone_source_to_raw',
        python_callable=task_bouches_du_rhone_source_to_raw,
    )

    t2 = PythonOperator(
        task_id='nord_source_to_raw',
        python_callable=task_nord_source_to_raw
    )

    t3 = PythonOperator(
        task_id='bouches_du_rhone_raw_to_formatted',
        python_callable=task_bouches_du_rhone_raw_to_formatted,
    )

    t4 = PythonOperator(
        task_id='nord_raw_to_formatted',
        python_callable=task_nord_raw_to_formatted
    )

    t5 = PythonOperator(
        task_id='produce_usage',
        python_callable=task_produce_usage
    )

    t6 = PythonOperator(
        task_id='index_to_elastic',
        python_callable=task_index_to_elastic
    )

    t1 >> t3
    t2 >> t4
    [t3, t4] >> t5
    t5 >> t6
