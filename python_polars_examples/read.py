# %% 
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
# %%
# don't handle the structure columns well
# patterns = pl.read_parquet("../parquet_db_format/pattern.parquet")
# places = pl.read_parquet("../parquet_db_format/places.parquet")
patterns = pl.from_arrow(pq.read_table("../parquet_db_format/pattern.parquet"))
places = pl.from_arrow(pq.read_table("../parquet_db_format/places.parquet"))

# %%
# delicate method that depends on identical rows for each structure.
hours = patterns\
    .select("bucketed_dwell_times")\
    .explode("bucketed_dwell_times")\
    .unnest("bucketed_dwell_times")

hours1 = patterns\
    .head(1)\
    .select("bucketed_dwell_times")\
    .explode("bucketed_dwell_times")\
    .unnest("bucketed_dwell_times")

output = patterns.select("placekey").to_series().to_list()
unique_vals = hours1.shape[0]
res = [ele for ele in output for i in range(unique_vals)]

hours\
    .with_columns(pl.Series(name = "placekey", values = res))

# %%
# robust method that handles different struct sizes
brands = patterns\
    .select("placekey", "related_same_day_brand")

brands_explode = pl.DataFrame({"key": "empty", "value": 1, "placekey": "empty"})
for i in range(brands.shape[0]):
    print(i)
    row_i = brands.row(i)
    df_i = pl.DataFrame(row_i[1])\
       .with_columns(pl.lit(row_i[0]).alias("placekey"))
    if df_i.shape[1] == 3:
        brands_explode = pl.concat([brands_explode, df_i], rechunk = True)
