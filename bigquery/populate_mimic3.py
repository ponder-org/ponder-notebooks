import modin.pandas as pd
import ponder; ponder.init()
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.oauth2 import service_account
import json

db_con = dbapi.Connection(
            bigquery.Client(
            credentials=service_account.Credentials.from_service_account_info(
                    json.loads(open("credential.json").read()),
                    scopes=["https://www.googleapis.com/auth/bigquery"],
                )
            )
        )

ponder.configure(bigquery_dataset='MIMIC3', default_connection=db_con)

def try_upload_csv(db_con,url,name):
    df = pd.read_csv(url)
    try:
        df.to_sql(name,db_con,index=False)
        print(f"Uploaded dataset to {name}")
    except ValueError as e:
        print(f"MIMC3.{name} already exists")

try_upload_csv(db_con,"https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ICUSTAYS.csv", "ICUSTAYS")
try_upload_csv(db_con,"https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/PATIENTS.csv","PATIENTS")
try_upload_csv(db_con,"https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ADMISSIONS.csv","ADMISSIONS")