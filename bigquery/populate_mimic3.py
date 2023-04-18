import ponder.bigquery
import modin.pandas as pd
import ponder; ponder.init()
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.oauth2 import service_account
import json

bigquery_con = dbapi.Connection(
            bigquery.Client(
            credentials=service_account.Credentials.from_service_account_info(
                    json.loads(open("../credential.json").read()),
                    scopes=["https://www.googleapis.com/auth/bigquery"],
                )
            )
        )

ponder.configure(bigquery_dataset='MIMIC3', default_connection=bigquery_con)

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ICUSTAYS.csv", on_bad_lines='skip')
tmp.to_sql("ICUSTAYS",bigquery_con,index=False)

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/PATIENTS.csv", on_bad_lines='skip')
tmp.to_sql("PATIENTS",bigquery_con,index=False)

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ADMISSIONS.csv", on_bad_lines='skip')
tmp.to_sql("ADMISSIONS",bigquery_con,index=False)