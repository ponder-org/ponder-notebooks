import ponder
ponder.init()

import duckdb
duckdb_con = duckdb.connect("ponder.db")
ponder.configure(default_connection=duckdb_con)

import modin.pandas as pd

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/citibike_tutorial.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_CITIBIKE",duckdb_con,index=False)
print("Uploaded dataset to PONDER_CITIBIKE")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/books.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_BOOKS",duckdb_con,index=False)
print("Uploaded dataset to PONDER_BOOKS")

# Upload TPC-H
df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/customer.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_CUSTOMER",duckdb_con,index=False)
print("Uploaded dataset to PONDER_CUSTOMER")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/orders.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_ORDERS",duckdb_con,index=False)
print("Uploaded dataset to PONDER_ORDER")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/part.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_PART",duckdb_con,index=False)
print("Uploaded dataset to PONDER_PART")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/supplier.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_SUPPLIER",duckdb_con,index=False)
print("Uploaded dataset to PONDER_SUPPLIER")
duckdb_con.close()
