# %%
import pandas as pd
import numpy as np

import safegraph_functions as sgf

# %%
pathLoc = "SafeGraph - Patterns and Core Data - Chipotle - July 2021/Core Places and Patterns Data/chipotle_core_poi_and_patterns.csv"
dat = pd.read_csv(pathLoc)

# %%
# fix dates
# 2021-08-01T00:00:00-05:00 for date_range
# 2019-07 closed_since
# ['date_range_start', 'date_range_end', 'tracking_closed_since']
dat = dat.assign(
    date_range_start = lambda x: pd.to_datetime(x.date_range_start.str.split("T").str[0], utc=True),
    date_range_end = lambda x: pd.to_datetime(x.date_range_end.str.split("T").str[0], utc=True),
    tracking_closed_since = lambda x: pd.to_datetime(x.tracking_closed_since, format= "%Y-%m")
 )



# %%
# complex columns
list_cols = ['visits_by_day', 'popularity_by_hour']
json_cols = ['open_hours', 'bucketed_dwell_times', 'related_same_day_brand', 'related_same_month_brand', 'popularity_by_day', 'device_type', 'visitor_home_aggregation', 'visitor_home_cbgs', 'visitor_country_of_origin','visitor_daytime_cbgs']

# %%
# base dataset
dat_base = dat.drop(list_cols + json_cols, axis=1)

dat_base.to_parquet("parquet/poi.parquet")

# %%
# only two list columns
dat_vbd = sgf.expand_list("visits_by_day", dat)
dat_pbh = sgf.expand_list("popularity_by_hour", dat)

dat_pbh.to_parquet("parquet/popularity_by_hour.parquet")

dat_vbd.to_parquet("parquet/visits_by_day.parquet")


# %%
# build tables
# my expand_json function feels super slow. took ~90 minutes to run. The visitor cbgs columns are the beast.

for i in json_cols:
    print(i)
    dati = sgf.expand_json(i, dat)
    dati.to_parquet("parquet/" + i + ".parquet")
 
    
# %%
# example of the expand_json function
dat_pbd = sgf.expand_json('popularity_by_day', dat, wide=False)
dat_rsdb = sgf.expand_json('related_same_day_brand', dat, wide=False)


dat_vhcbgs = sgf.expand_json('visitor_home_cbgs', dat.iloc[:100,:], wide=False)


# %%
