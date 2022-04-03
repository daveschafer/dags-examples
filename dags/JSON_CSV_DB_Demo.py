# V2 with postgress DB connector and postgress operator by airflow
#
# The Postgres Operator module can work with .sql files but not directly with .csv, so we would need to convert this a bit
# Therefore we use the Postgres Hook which can do this for us with "copy_experts" function
#
# The createTableLoad was adjusted therefore
#
# To still have a usecase for Postgres Operator, at the end we select the top 3 entries from the now loaded table (fetch) and just display them (not really useful)
# We will then get the Fetched Data via pythonoperator and airflows xcom and process them (do something like modify the names)
# And finally we will save them again into another csv
# # 
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
#Additional Postgres Operator for Fetching
from airflow.providers.postgres.operators.postgres import PostgresOperator
#Additional Postgres Hook for CSV Import
from airflow.hooks.postgres_hook import PostgresHook
#Additional HTTP Sensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

# Operators (working load) imports
import requests
import json
import pandas as pd

import psycopg2 as pg
import csv

#some better logging
import logging
logger = logging.getLogger("airflow.task")

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
#--> is now under CustomOperators.PythonOperators.RESTHandler
import CustomOperators.PythonOperators.RESTHandler as RESTHandler

# Read temporary CSV and store it in Postgres DB
# V1: Credentials to Postgres DB are specified here
def loadDataIntoTable():
    try:
        # Connect to Database with Postgres.hook instead of manually define Connection String
        # The connection has to be created in Airflow UI first
        pg_hook = PostgresHook(
            postgres_conn_id='postgres_dezyre_new'
        )
        pg_conn = pg_hook.get_conn()
        #dbconnect = pg.connect("dbname='dezyre_new' user='postgres' host='localhost' password='mysecretpassword'")
        print("yaii success, connected to DB :-)")
        logger.info(f"Connected to db '{str(pg_hook.conn)}'")

    except Exception as error:
        logger.error("Task 'getDataToLocal' run into an error")


    # create db cursor
    cursor = pg_conn.cursor()

    # insert each csv row as a record in our database
    with open("/home/dave/airflow/temp/drivers2.csv", "r") as f:
        print("Now loading data into drivers_data2 table")
        next(f)  # skip the first row (header)
        for row in f:
            cursor.execute(
                """
                INSERT INTO drivers_data2
                VALUES ('{}', '{}', '{}', '{}', '{}')
                """.format(
                    row.split(",")[0], row.split(",")[1], row.split(",")[2], row.split(",")[3], row.split(",")[4]
                )
            )
    logger.info("Task 'getDataToLocal' done")
    pg_conn.commit()

# Fetching Data from DB:
def fetchDataFromDB():
    sql_stmt = "SELECT * FROM drivers_data2 LIMIT 3;" #only first 3 records
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_dezyre_new'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    logger.info("Task 'fetchDataFromDB' done")
    return cursor.fetchall() #this can be consumed via xcom

# Get data via xcom, modify them and save to another csv.
def receiveModifyAndExport2CSV(ti):
    #G
    drivers_data = ti.xcom_pull(task_ids=['fetchDataFromDB']) #magic
    if not drivers_data:
        raise Exception('No data.')
    #TODO
    drivers_data = pd.DataFrame(
        data=drivers_data[0],
        columns=['school_year', 'vendor_name', 'type_of_service',
                 'active_employees', 'job_type']
    )
    print("*****")
    print(drivers_data[(drivers_data['active_employees'] != "1")] )
    print("*****")
    drivers_data = drivers_data[
        (drivers_data['active_employees'] != "1") 
    ] #this wil drop 1 entry and only 2 stay (with 3 and 5)

    #and store anew
    drivers_data = drivers_data.drop('type_of_service', axis=1) #this will drop the whole column "type_of_service"
    #remove messy newline chars
    drivers_data = drivers_data.replace(r'\n',' ', regex=True) 
    #write to a new "processed csv"
    drivers_data.to_csv("/home/dave/airflow/temp/drivers2_processed.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
    logger.info("CSV Exportet to: '/home/dave/airflow/temp/drivers2_processed.csv'")

# Dynamic function with param
def dynamicFunction(parameter):
    print(f"This DynamicFunction was called with parameter: {parameter}")

################################################
## 4. Instantiate/Create the DAG (Definition) ##
################################################

dag_JSON_CSV_DB = DAG(
    dag_id="JSON_CSV_DB_Demo",  # aka the DAG name
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval="@once",
    dagrun_timeout=timedelta(minutes=60),
    description="use case of pandas  in airflow with postgres.hook and postgres.operator",
    start_date=airflow.utils.dates.days_ago(1),
)
###############################
## 5 Define Tasks within DAG ##
###############################

# 0. Check if HTTP Site is available

task_http_sensor_check = HttpSensor(
    task_id='task_http_sensor_check',
    http_conn_id='http_example_api',
    endpoint='4tqt-y424.json',
    dag=dag_JSON_CSV_DB,
)

# 1. Web JSON to CSV
# 1.1 Classical Python Way
getDataToLocal_pytask = PythonOperator(task_id="getDataToLocal_pytask", python_callable=RESTHandler.getDataToLocal, dag=dag_JSON_CSV_DB)
# 1.2.1 Alternatively we can first fetch the Data from REST API with SimpleHTTPOperator --> but then need to pass it to another processing function (this will be storeToCSV)
# Note that SimpleHttpOperator only can work with JSON returns therefore we are using another URL here (the other one responds text/csv)
getDataFromWeb = SimpleHttpOperator(
    task_id='getDataFromWeb',
    http_conn_id='http_example_api',
    endpoint='4tqt-y424.json',
    method='GET',
    #SimpleHttpOperator returns the response body as text by default
    response_filter=lambda response: json.loads(response.text),
    log_response=True,
    do_xcom_push=True,
    dag=dag_JSON_CSV_DB,
)

#1.2.2 Get the data via XCOM and store it to CSV
storeToCSV_task = PythonOperator(task_id="storeToCSV_task", python_callable=RESTHandler.storeToCSV_task, dag=dag_JSON_CSV_DB)

# 2. Create Table if not Existing (Postgres Operator)
create_table_ifnotexist = PostgresOperator(
    task_id="create_table_ifnotexist",
    postgres_conn_id="postgres_dezyre_new", #this we need to specify in airflow
    sql="""
         CREATE TABLE IF NOT EXISTS drivers_data2 (
            school_year varchar(50),
            vendor_name varchar(50),
            type_of_service varchar(50),
            active_employees varchar(50),
            job_type varchar(50)
        );
        
        TRUNCATE TABLE drivers_data2;
    """,
    dag=dag_JSON_CSV_DB,
) 
# 3. CSV into Postgres
loadDataIntoTable = PythonOperator(task_id="loadDataIntoTable", python_callable=loadDataIntoTable, dag=dag_JSON_CSV_DB)

# 4. Fetch Postgres Data from and Provide them via xCOM
fetchDataFromDB = PythonOperator(task_id="fetchDataFromDB", python_callable=fetchDataFromDB, do_xcom_push=True, dag=dag_JSON_CSV_DB)

# 5. Receive fetched Data via XCOM and save them to another CSV File
receiveModifyAndExport2CSV = PythonOperator(task_id="receiveModifyAndExport2CSV", python_callable=receiveModifyAndExport2CSV, dag=dag_JSON_CSV_DB)

# 6. at the End some dynamic task example
letters = ['A','B','C']
numbers = ["1","2","T"]
dynamicTaskExample = [
    PythonOperator(
        task_id=f"dynamicTaskExample_{letter}",
        python_callable=dynamicFunction,
        op_args=numbers[i] #which is like calling 'dynamicFunction(paremterx)'
    ) for i, letter in enumerate(letters)]


# 6.1 Another approach to dynamically start some tasks
# This approach is actually easier and should be preferred!
import time
from airflow.operators.python import task

for i in range(5):
    @task(task_id=f'dynamic_sleep_for_{i}')
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)

    sleeping_task = my_sleeping_function(random_base=float(i) / 10)

    create_table_ifnotexist >> sleeping_task

########
# Test your Tasks: 
#
#   airflow tasks test JSON_CSV_DB_Demo <task> 2022-01-01
#
#######

####################################################
## 6. Define Depencendies (Reihenfolge der Tasks) ##
####################################################

task_http_sensor_check >> getDataFromWeb >> storeToCSV_task >> create_table_ifnotexist >> loadDataIntoTable >> fetchDataFromDB >> receiveModifyAndExport2CSV
task_http_sensor_check >> getDataToLocal_pytask
storeToCSV_task >> dynamicTaskExample

# 6.1 - Entrypoint (is this really needed?)

if __name__ == "__main__ ":
    dag_JSON_CSV_DB.cli()
