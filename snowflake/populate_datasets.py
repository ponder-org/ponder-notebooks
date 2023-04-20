import credential
import ponder
ponder.init()
import modin.pandas as pd

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

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/citibike_trial.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_CITIBIKE",snowflake_con,index=False)
print("Uploaded dataset to PONDER_CITIBIKE")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/books.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_BOOKS",snowflake_con,index=False)
print("Uploaded dataset to PONDER_BOOKS")

# Upload TPC-H
df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/customer.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_CUSTOMER",snowflake_con,index=False)
print("Uploaded dataset to PONDER_CUSTOMER")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/orders.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_ORDERS",snowflake_con,index=False)
print("Uploaded dataset to PONDER_ORDER")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/part.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_PART",snowflake_con,index=False)
print("Uploaded dataset to PONDER_PART")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/supplier.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_SUPPLIER",snowflake_con,index=False)
print("Uploaded dataset to PONDER_SUPPLIER")
