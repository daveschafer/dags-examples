#V1 - this approach does not directly use PostgresOperator since it has to do transformations (which is really easy in pandas/python but not with the Postgres Operator)
##########################################
## 1. Import relevant libraries for DAG ##
##########################################

# Airflow DAG specific imports
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

# Operators (working load) imports
import requests
import json
import pandas as pd

# this approach uses the "old fashioned" way of importing psycopg2
import psycopg2 as pg
import csv

# 1.1. Define variables/configuration

######################################
## 2. Set Default Arguments for DAG ##
######################################


args = {"owner": "airflow"}

default_args = {
    "owner": "airflow",
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

################################
## 3. Python functions needed ##
################################


# Get JSON from Web and store it in a temporary CSV
def getDataToLocal():
    url = "https://data.cityofnewyork.us/resource/4tqt-y424.json"
    response = requests.get(url)

    df = pd.DataFrame(json.loads(response.content))
    df = df.set_index("school_year")

    df.to_csv("/home/dave/airflow/temp/drivers.csv", sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, encoding="utf-8")


# Read temporary CSV and store it in Postgres DB
# V1: Credentials to Postgres DB are specified here
def creatableLoad():
    try:
        # do i need to create the db manually first?
        dbconnect = pg.connect("dbname='dezyre_new' user='postgres' host='localhost' password='mysecretpassword'")
        print("yaii success, connected to DB :-)")
    except Exception as error:
        print("We run into an error :-/")
        print(error)

    # create the table if it does not already exist
    cursor = dbconnect.cursor()
    cursor.execute(
        """
         CREATE TABLE IF NOT EXISTS drivers_data (
            school_year varchar(50),
            vendor_name varchar(50),
            type_of_service varchar(50),
            active_employees varchar(50),
            job_type varchar(50)
        );
        
        TRUNCATE TABLE drivers_data;
    """
    )
    dbconnect.commit()

    # insert each csv row as a record in our database
    with open("/home/dave/airflow/temp/drivers.csv", "r") as f:
        next(f)  # skip the first row (header)
        for row in f:
            cursor.execute(
                """
                INSERT INTO drivers_data
                VALUES ('{}', '{}', '{}', '{}', '{}')
            """.format(
                    row.split(",")[0], row.split(",")[1], row.split(",")[2], row.split(",")[3], row.split(",")[4]
                )
            )
    dbconnect.commit()

################################################
## 4. Instantiate/Create the DAG (Definition) ##
################################################

dag_pandas = DAG(
    dag_id="using_pandas_demo",  # aka the DAG name
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval="@once",
    dagrun_timeout=timedelta(minutes=60),
    description="use case of pandas  in airflow",
    start_date=airflow.utils.dates.days_ago(1),
)
###############################
## 5 Define Tasks within DAG ##
###############################

getDataToLocal = PythonOperator(task_id="getDataToLocal", python_callable=getDataToLocal, dag=dag_pandas)

creatableLoad = PythonOperator(task_id="creatableLoad", python_callable=creatableLoad, dag=dag_pandas)

####################################################
## 6. Define Depencendies (Reihenfolge der Tasks) ##
####################################################

getDataToLocal >> creatableLoad

# 6.1 - Entrypoint (is this really needed?)

if __name__ == "__main__ ":
    dag_pandas.cli()
