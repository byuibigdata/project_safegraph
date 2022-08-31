# %%
import pandas as pd
import duckdb
import os

# import pyarrow as pa
# import pyarrow.parquet as pq
# %%
con = duckdb.connect(database='chipotle_july.duckdb', read_only=False)

# dat = pd.read_parquet("parquet/poi.parquet")
# con.register("poi", dat)
# https://deepnote.com/@abid/Data-Science-with-DuckDB-9KKvj1EoQrmj6nj4Y2prkg
# https://benschmidt.org/post/2021-04-28-duckworm/2021-04-28-duckworm/
# %%
# https://duckdb.org/docs/data/parquet
pfiles = os.listdir('parquet')

for i in pfiles:
    print(i)
    tablei = i.replace('.parquet', '')
    pathi = 'parquet/' + i
    query = f"""
    CREATE TABLE {tablei} AS SELECT * FROM '{pathi}';
    """
    con.execute(query)
    

# %%
con.commit()

# %%
con.execute("SHOW TABLES").fetchall()

# %%
con.execute("SELECT * FROM visitor_daytime_cbgs LIMIT 25").fetchdf()

# %%
con.close()

# %%
