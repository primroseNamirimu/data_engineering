from airflow.decorators import dag, task
from datetime import datetime
import packages.functions 

@dag(schedule_interval=None, start_date=datetime(2025, 3, 28), catchup=False)
def my_etl_dag():
    @task()
    def extract_task():
        return packages.functions.extract()

    @task()
    def transform_task(data):
        return packages.functions.transform(*data)

    @task()
    def load_task(transformed_data):
        return packages.functions.load(transformed_data)

    # Define DAG flow
    extracted_data = extract_task()
    transformed_data = transform_task(extracted_data)
    load_task(transformed_data)

my_etl_dag()