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
db_con = snowflake.connector.connect(
    user=credential.params["user"],
    password=credential.params["password"],
    account=credential.params["account"],
    role=credential.params["role"],
    database=credential.params["database"],
    schema=credential.params["schema"],
    warehouse=credential.params["warehouse"]
)
ponder.configure(default_connection=db_con)


import modin.pandas as pd

def try_upload_csv(db_con,url,name):
    df = pd.read_csv(url)
    try:
        df.to_sql(name,db_con,index=False)
        print(f"Uploaded dataset to {name}")
    except ValueError as e:
        print(f"{name} already exists")

try_upload_csv(db_con,"https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ICUSTAYS.csv", "ICUSTAYS")
try_upload_csv(db_con,"https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/PATIENTS.csv","PATIENTS")
try_upload_csv(db_con,"https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ADMISSIONS.csv","ADMISSIONS")