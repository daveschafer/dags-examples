import logging
import requests
import pandas as pd
import csv
import json
logger = logging.getLogger("airflow.task")


def storeToCSV(content, isJSON=False):
    if isJSON:
        df = pd.DataFrame(content[0]) #somehow we get a list in list from XCOM
    else:
        df = pd.DataFrame(json.loads(content))
    df = df.set_index("school_year")

    df.to_csv("/tmp/drivers2.csv", sep=",", escapechar="\\", quoting=csv.QUOTE_NONE, encoding="utf-8")
    logger.info("Task 'storeToCSV' done")


#Wrapper which gets the data from XCOM and then passes to storeToCSV
def storeToCSV_task(ti) -> None:
    content = ti.xcom_pull(task_ids=['getDataFromWeb'])
    logger.info("******")
    print("Content type is: ")
    print(type(content))
    if type(content) == list:
        logger.info("Passing JSON ")
        storeToCSV(content=content,isJSON=True)
    else:
        logger.info("Passing raw content")
        storeToCSV(content=content)

def getDataToLocal():
    url = "https://data.cityofnewyork.us/resource/4tqt-y424.json"
    response = requests.get(url)

    storeToCSV(response.content)
    logger.info("Task 'getDataToLocal' done")

