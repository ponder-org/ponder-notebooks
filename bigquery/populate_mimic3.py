import ponder.bigquery
import modin.pandas as pd
import json; import os; 
creds = json.load(open(os.path.expanduser("credential.json")))
# Create a Ponder BigQuery Connections object
bigquery_con = ponder.bigquery.connect(creds, schema = "TEST")
# Initialize the BigQuery connection
ponder.bigquery.init(bigquery_con)

import modin.pandas as pd

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ICUSTAYS.csv", on_bad_lines='skip')
tmp.to_sql("ICUSTAYS",bigquery_con,index=False)

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/PATIENTS.csv", on_bad_lines='skip')
tmp.to_sql("PATIENTS",bigquery_con,index=False)

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ADMISSIONS.csv", on_bad_lines='skip')
tmp.to_sql("ADMISSIONS",bigquery_con,index=False)