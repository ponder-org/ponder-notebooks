import ponder.duckdb
import modin.pandas as pd

duckdb_con = ponder.duckdb.connect("ponder.db", client_row_transfer_limit=1000000000,  read_only = False)
ponder.duckdb.init(duckdb_con, client_row_transfer_limit=1000000000, timeout=60000)

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/citibike_trial.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_CITIBIKE",duckdb_con,index=False)
print("Uploaded dataset to PONDER_CITIBIKE")


# df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/books.csv?raw=True", on_bad_lines='skip')
# df.to_sql("PONDER_BOOKS",duckdb_con,index=False)
# print("Uploaded dataset to PONDER_BOOKS")


df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/yellow_tripdata_2015-01.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_TAXI",duckdb_con,index=False)
print("Uploaded dataset to PONDER_TAXI")

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
