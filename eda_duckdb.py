# %%
import pandas as pd
import duckdb
import os

# import pyarrow as pa
# import pyarrow.parquet as pq
# %%
con = duckdb.connect(database='chipotle_july.duckdb', read_only=False)
# %%
# https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html

con.execute("SHOW TABLES").fetchall()
# %%
vh = con.execute("SELECT * FROM visitor_home_cbgs").fetchdf()
# %%
