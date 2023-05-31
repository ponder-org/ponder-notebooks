import ponder
ponder.init()

import duckdb
db_con = duckdb.connect("ponder.db")
ponder.configure(default_connection=db_con)

import modin.pandas as pd

def try_upload_csv(db_con,url,name):
    df = pd.read_csv(url)
    try:
        df.to_sql(name,db_con,index=False)
        print(f"Uploaded dataset to {name}")
    except ValueError as e:
        print(f"{name} already exists")

try_upload_csv(db_con,"https://github.com/ponder-org/ponder-datasets/blob/main/citibike_tutorial.csv?raw=True", "PONDER_CITIBIKE")
try_upload_csv(db_con,"https://github.com/ponder-org/ponder-datasets/blob/main/books.csv?raw=True", "PONDER_BOOKS")
# Upload TPC-H
try_upload_csv(db_con,"https://github.com/ponder-org/ponder-datasets/blob/main/tpch/customer.csv?raw=True","PONDER_CUSTOMER")
try_upload_csv(db_con,"https://github.com/ponder-org/ponder-datasets/blob/main/tpch/orders.csv?raw=True","PONDER_ORDERS")
try_upload_csv(db_con,"https://github.com/ponder-org/ponder-datasets/blob/main/tpch/part.csv?raw=True","PONDER_PART")
try_upload_csv(db_con,"https://github.com/ponder-org/ponder-datasets/blob/main/tpch/supplier.csv?raw=True","PONDER_SUPPLIER")

db_con.close()
