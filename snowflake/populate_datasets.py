import credential
import ponder
ponder.init()
import modin.pandas as pd

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

