import ponder
ponder.init()

import duckdb
duckdb_con = duckdb.connect("mimic3.db")
ponder.configure(default_connection=duckdb_con)

import modin.pandas as pd

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ICUSTAYS.csv")
tmp.to_sql("ICUSTAYS",duckdb_con,index=False)
print("Uploaded dataset to ICUSTAYS")

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/PATIENTS.csv")
tmp.to_sql("PATIENTS",duckdb_con,index=False)
print("Uploaded dataset to PATIENTS")

tmp = pd.read_csv("https://raw.githubusercontent.com/ponder-org/ponder-datasets/main/mimic-iii/ADMISSIONS.csv")
tmp.to_sql("ADMISSIONS",duckdb_con,index=False)
print("Uploaded dataset to ADMISSIONS")

duckdb_con.close()