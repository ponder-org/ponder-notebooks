import ponder
ponder.init()

import duckdb
db_con = duckdb.connect("mimic3.db")
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
db_con.close()