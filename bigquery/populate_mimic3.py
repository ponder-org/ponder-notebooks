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
from ponder.utils.core import Teleporter
t = Teleporter()
remote_path = t.depulso("mimic-iii-clinical-database-demo-1.4/ICUSTAYS.csv")

tmp = pd.read_csv(remote_path, on_bad_lines='skip')
tmp.to_sql("ICUSTAYS",bigquery_con,index=False)

t = Teleporter()
remote_path = t.depulso("mimic-iii-clinical-database-demo-1.4/PATIENTS.csv")
tmp = pd.read_csv(remote_path, on_bad_lines='skip')
tmp.to_sql("PATIENTS",bigquery_con,index=False)

t = Teleporter()
remote_path = t.depulso("mimic-iii-clinical-database-demo-1.4/ADMISSIONS.csv")
tmp = pd.read_csv(remote_path, on_bad_lines='skip')
tmp.to_sql("ADMISSIONS",bigquery_con,index=False)