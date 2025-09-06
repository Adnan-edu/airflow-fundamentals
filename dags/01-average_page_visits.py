from datetime import datetime
import os
import json
import random

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
    "average_page_visits", # Id of the DAG
    start_date=datetime(2025, 9, 2), # Start date of the DAG - First execution
    schedule_interval="* * * * *", # Schedule interval of the DAG - Every minute
    catchup=False, # catchup=False means Airflow will NOT run DAG runs for any intervals between start_date and now that were missed while the scheduler was not running.
                   # For example, if today is 2025-01-10 and you set start_date to 2025-01-01 with schedule_interval every minute:
                   #   - If catchup=True (default), Airflow will try to "catch up" and run all missed DAG runs from 2025-01-01 up to now.
                   #   - If catchup=False, Airflow will only run the DAG from the moment you turn it on, ignoring all previous intervals.
)
def average_page_visits():

    def get_data_path():
        context = get_current_context()
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H%M")
        return f"/tmp/page_visits/{file_date}.json"

    @task
    def produce_page_visits_data():

        # if random.random() < 0.5:
        #     raise Exception("Job has failed")

        page_visits = [
            {"id": 1, "name": "Cozy Apartment", "price": 120, "page_visits": random.randint(0, 50)},
            {"id": 2, "name": "Luxury Condo", "price": 300, "page_visits": random.randint(0, 50)},
            {"id": 3, "name": "Modern Studio", "price": 180, "page_visits": random.randint(0, 50)},
            {"id": 4, "name": "Charming Loft", "price": 150, "page_visits": random.randint(0, 50)},
            {"id": 5, "name": "Spacious Villa", "price": 400, "page_visits": random.randint(0, 50)},
        ]

        file_path = get_data_path()

        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, "w") as f:
            json.dump(page_visits, f)

        print(f"Written to file: {file_path}")

    @task
    def process_page_visits_data():
        file_path = get_data_path()

        with open(file_path, "r") as f:
            page_visits = json.load(f)

        average_price = sum(page_visit["page_visits"] for page_visit in page_visits) / len(page_visits)
        print(f"Average number of page visits: {average_price}")

    produce_page_visits_data() >> process_page_visits_data()



demo_dag = average_page_visits()