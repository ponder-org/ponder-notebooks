import credential
credential.params["database"] = "MIMIC3"
# Create Database MIMIC3
# ! pip install --upgrade snowflake-sqlalchemy
from snowflake.sqlalchemy import URL
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

import ponder
ponder.init()

import snowflake.connector
snowflake_con = snowflake.connector.connect(
    user=credential.params["user"],
    password=credential.params["password"],
    account=credential.params["account"],
    role=credential.params["role"],
    database=credential.params["database"],
    schema=credential.params["schema"],
    warehouse=credential.params["warehouse"]
)
ponder.configure(default_connection=snowflake_con)


import modin.pandas as pd

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ICUSTAYS.csv", on_bad_lines='skip')
tmp.to_sql("ICUSTAYS",snowflake_con,index=False)
print("Uploaded dataset to ICUSTAYS")

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/PATIENTS.csv", on_bad_lines='skip')
tmp.to_sql("PATIENTS",snowflake_con,index=False)
print("Uploaded dataset to PATIENTS")

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ADMISSIONS.csv", on_bad_lines='skip')
tmp.to_sql("ADMISSIONS",snowflake_con,index=False)
print("Uploaded dataset to ADMISSIONS")