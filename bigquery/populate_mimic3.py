import credential
import ponder.bigquery
credential.params["database"] = "MIMIC3"
# Create Database MIMIC3
# ! pip install --upgrade bigquery-sqlalchemy
from bigquery.sqlalchemy import URL
from sqlalchemy import create_engine
engine = create_engine(URL(
    account = credential.params["account"],
    user = credential.params["user"],
    password = credential.params["password"],
    database = credential.params["database"],
    schema = credential.params["schema"],
    warehouse = credential.params["warehouse"],
    role=credential.params["role"],
))
connection = engine.connect()
connection.execute("CREATE DATABASE IF NOT EXISTS MIMIC3;") 


# Create a Ponder BigQuery Connections object
bigquery_con = ponder.bigquery.connect(
    user=credential.params["user"],
    password=credential.params["password"],
    account=credential.params["account"],
    role=credential.params["role"],
    database=credential.params["database"],
    schema=credential.params["schema"],
    warehouse=credential.params["warehouse"]
)
# Initialize the BigQuery connection
ponder.bigquery.init(bigquery_con,enable_ssl=True)

import modin.pandas as pd

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ICUSTAYS.csv", on_bad_lines='skip')
tmp.to_sql("ICUSTAYS",bigquery_con,index=False)

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/PATIENTS.csv", on_bad_lines='skip')
tmp.to_sql("PATIENTS",bigquery_con,index=False)

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ADMISSIONS.csv", on_bad_lines='skip')
tmp.to_sql("ADMISSIONS",bigquery_con,index=False)