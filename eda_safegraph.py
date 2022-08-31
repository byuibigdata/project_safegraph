# %%
import pandas as pd
import numpy as np
import geopandas as gpd
from plotnine import *
import safegraph_functions as sgf
# %%
pathLoc = "SafeGraph - Patterns and Core Data - Chipotle - July 2021/Core Places and Patterns Data/chipotle_core_poi_and_patterns.csv"
dat = pd.read_csv(pathLoc)

datl = dat.iloc[:10,:]
# %%
list_cols = ['visits_by_day', 'popularity_by_hour']
json_cols = ['open_hours','visitor_home_cbgs', 'visitor_country_of_orgin', 'bucketed_dwell_times', 'related_same_day_brand', 'related_same_month_brand', 'popularity_by_day', 'device_type', 'visitor_home_aggregation', 'visitor_daytime_cbgs']

dat_pbd = sgf.expand_json('popularity_by_day', datl)
dat_rsdb = sgf.expand_json('related_same_day_brand', datl)
dat_vbd = sgf.expand_list("visits_by_day", datl)
dat_pbh = sgf.expand_list("popularity_by_hour", datl)

# %%
# What are the top three brands that Chipotle customers visit on the same day?
# Create a bar chart of the top 10 to show us.
dat_rsdb = sgf.expand_json('related_same_day_brand', dat)
# %%
dat20 = (dat_rsdb
    .drop(columns = ["placekey"])
    .sum()
    .reset_index()
    .rename(columns = {"index":"brand", 0:"visits"})
    .sort_values(by = "visits", ascending = False)
    .assign(brand = lambda x: x.brand.str.replace("related_same_day_brand-", ""))
    .head(20)
    .reset_index(drop=True)
)
# %%
# you should figure out how to sort your barchart
(ggplot(dat20, aes(x = "brand", y = "visits")) +
geom_col() +
coord_flip())
# %%
# Over the hours in a day which has to most variability across the Chipotle brand? Which has the highest median visit rate?
# Create a boxplot by hour of the day to help answer this question
dat_pbh = sgf.expand_list("popularity_by_hour", dat)
# %%
(ggplot(dat_pbh, aes(x = "hour.astype(str).str.zfill(2)", y = "popularity_by_hour")) +
geom_boxplot() +
scale_y_continuous(limits = [0, 100]))
# %%
