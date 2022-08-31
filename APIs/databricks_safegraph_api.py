# %%
import pandas as pd

import json
import os
import jsonlines
import re
import functools
import itertools
import shutil
import time

from multiprocessing import Pool
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

# from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# SafeGraph Key
from dotenv import load_dotenv

load_dotenv()
sfkey = os.environ.get("SAFEGRAPH_KEY")

# API URL
# https://docs.safegraph.com/reference/intro-to-graphql
url = 'https://api.safegraph.com/v2/graphql'

# Initiate API connection
transport = RequestsHTTPTransport(
    url=url,
    verify=True,
    retries=3,
    headers={'Content-Type': 'application/json', 'apikey': sfkey})

client = Client(transport=transport, fetch_schema_from_transport=True)


# %%

#  safegraph_weekly_patterns (date: "2021-07-12")
# "naics_code: 813110,"
query_sg = """query {
  search(filter: { 
    --FILTERS--
    address: {
      region: "--STATENAME--"
    }
  }){
    places {
      results(first: 500 after: "") {
        pageInfo { hasNextPage, endCursor}
        edges {
          node {
            safegraph_weekly_patterns (date: "--WEEKDATE--") {
              placekey
              parent_placekey
              location_name
              street_address
              city
              region
              postal_code
              iso_country_code
              date_range_start
              date_range_end
              raw_visit_counts
              raw_visitor_counts
              device_type
              poi_cbg
              visitor_home_cbgs
              visitor_home_aggregation
              visitor_daytime_cbgs
              visitor_country_of_origin
              distance_from_home
              median_dwell
              bucketed_dwell_times
              related_same_day_brand
              related_same_week_brand
            }
          }
        }
      }
    }
  }
}
"""

# weekly_patterns function

query_sg_week = """query {
  search(filter: { 
     --FILTERS--
    address: {
      region: "--STATENAME--"
    }
  }){
    places {
      results(first: 500 after: "") {
        pageInfo { hasNextPage, endCursor}
        edges {
          node {
            weekly_patterns (start_date: "--WEEKDATE--" end_date: "--WEEKDATEEND--") {
              placekey
              parent_placekey
              location_name
              street_address
              city
              region
              postal_code
              iso_country_code
              date_range_start
              date_range_end
              raw_visit_counts
              raw_visitor_counts
              device_type
              poi_cbg
              visitor_home_cbgs
              visitor_home_aggregation
              visitor_daytime_cbgs
              visitor_country_of_origin
              distance_from_home
              median_dwell
              bucketed_dwell_times
              related_same_day_brand
              related_same_week_brand
            }
          }
        }
      }
    }
  }
}
"""


# %%
state_list = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS',
              'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

date_list = pd.date_range(start = "2019-01-18", end = "2021", freq='W-MON').strftime('%Y-%m-%d').tolist()


stateDate = list(itertools.product(state_list, date_list))

base_path = "Colorado"

try:
  os.mkdir(base_path)
  print("Created Now")
except:
  print("Made earlier")

stateIter = 'CO'
COIter = list(itertools.product(['CO'], date_list))

# %%

def sg_import_jsonlines(stateDate, query_sg, base_path, naic_filter = None, printquery=False, nextPageStart=None):
  var_map = {'related_same_day_brand':'brand_day', 'related_same_week_brand':'brand_week', 'visitor_home_cbgs':'home_cbgs',
             'visitor_home_aggregation':'home_agg', 'visitor_daytime_cbgs':'daytime_cbgs', 'visitor_country_of_origin':'country'}

  stateIter = stateDate[0]
  weekIter = stateDate[1]
  
  weekIterEnd = pd.to_datetime(weekIter) + pd.DateOffset(days=1)
  weekIterEnd = weekIterEnd.strftime('%Y-%m-%d')

  if naic_filter == None:
    filter_text = ''
  else:
    # "naics_code: 813110,"
    filter_text = naic_filter[0] + ": " + naic_filter[1] + ',' 
  
  try:
    os.mkdir(base_path + "/" + stateIter)
  except:
    print("Made earlier")

  query_state = (query_sg.replace("--STATENAME--", stateIter)
                 .replace("--WEEKDATE--", weekIter)
                 .replace("--FILTERS--", filter_text)
                )

#   if nextPageStart == None:
#     resultsIter = client.execute(gql(query_state))
#   else: 
#     nextString = 'after: "' + nextPageStart + '"'
#     queryIter = query_state.replace('after: ""', nextString)
#     resultsIter = client.execute(gql(queryIter))

      

  if printquery:
    print(query_state)

  

  pageInformation = resultsIter['search']['places']['results']['pageInfo']
  pagestep = 0
  nextString = ''

  while (pageInformation['hasNextPage'] or (pagestep == 0)):
  
    if pagestep != 0:
      nextString = 'after: "' + pageInformation['endCursor'] + '"'
      queryIter = query_state.replace('after: ""', nextString)
      
      
      resultsIter = client.execute(gql(queryIter))
      
      
    else:
      print(str(pagestep) + ": First Call")
  
    pageInformation = resultsIter['search']['places']['results']['pageInfo']
    edgesIter = resultsIter['search']['places']['results']['edges']
    resultsIter = [dat.pop('node') for dat in edgesIter]
    resultsIter = [dat.pop('safegraph_weekly_patterns') for dat in resultsIter]

    for i, value in enumerate(resultsIter):
      brand = list()
      counts = list()
      for var, name  in var_map.items():
        if resultsIter[i] is not None:
          if resultsIter[i][var] is not None:
            tempi = resultsIter[i][var].copy()
            for k, v in tempi.items():
              brand.append(k)
              counts.append(v)
          resultsIter[i][var] = {name:brand, 'count':counts}

 
    pathString = base_path + "/" + stateIter + "/" + 'json_' + weekIter + "_" + str(pagestep) + '.jl'
    
    
    with jsonlines.open(pathString, 'w') as writer:
      writer.write_all(resultsIter)
      writer.close()
    print(str(pagestep) + "stuff: " + pageInformation['endCursor'] + " , ", end =" ")
    pagestep += 1
  return (stateIter, pagestep)
    

def sg_import_jsonlines_2(stateDate, query_sg, base_path, naic_filter = None, printquery=False):
  var_map = {'related_same_day_brand':'brand_day', 'related_same_week_brand':'brand_week', 'visitor_home_cbgs':'home_cbgs',
             'visitor_home_aggregation':'home_agg', 'visitor_daytime_cbgs':'daytime_cbgs', 'visitor_country_of_origin':'country'}

  stateIter = stateDate[0]
  weekIter = stateDate[1]
  
  weekIterEnd = pd.to_datetime(weekIter) + pd.DateOffset(days=1)
  weekIterEnd = weekIterEnd.strftime('%Y-%m-%d')
  
  if naic_filter == None:
    filter_text = ''
  else:
    # "naics_code: 813110,"
    filter_text = naic_filter[0] + ": " + naic_filter[1] + ',' 
  
  try:
    os.mkdir(base_path + "/" + stateIter)
  except:
    print("Made earlier")

  query_state = (query_sg.replace("--STATENAME--", stateIter)
                 .replace("--WEEKDATE--", weekIter)
                 .replace("--FILTERS--", filter_text)
                 .replace("--WEEKDATEEND--", weekIterEnd)
                )
  
  if printquery:
    print(query_state)

  resultsIter = client.execute(gql(query_state))

  pageInformation = resultsIter['search']['places']['results']['pageInfo']
  pagestep = 0
  nextString = ''

  while (pageInformation['hasNextPage'] or (pagestep == 0)):
  
    if pagestep != 0:
      nextString = 'after: "' + pageInformation['endCursor'] + '"'
      queryIter = query_state.replace('after: ""', nextString)
      
      
      resultsIter = client.execute(gql(queryIter))
      
      
    else:
      print(str(pagestep) + ": First Call")
  
    pageInformation = resultsIter['search']['places']['results']['pageInfo']
    edgesIter = resultsIter['search']['places']['results']['edges']
    resultsIter = [dat.pop('node') for dat in edgesIter]
    resultsIter = [dat.pop('weekly_patterns') for dat in resultsIter]

    for i, value in enumerate(resultsIter):
      brand = list()
      counts = list()
      for var, name  in var_map.items():
        if resultsIter[i] is not None:
          if resultsIter[i][0][var] is not None:
            tempi = resultsIter[i][0][var].copy()
            for k, v in tempi.items():
              brand.append(k)
              counts.append(v)
          resultsIter[i][0][var] = {name:brand, 'count':counts}

 
    pathString = base_path + "/" + stateIter + "/" + 'json_' + weekIter + "_" + str(pagestep) + '.jl'
    
    
    with jsonlines.open(pathString, 'w') as writer:
      writer.write_all(resultsIter)
      writer.close()
    print(str(pagestep) + " : " + nextString + " , ", end =" ")
    pagestep += 1
  return (stateIter, pagestep)
    
        

# %%
sgijl = functools.partial(sg_import_jsonlines, base_path=base_path, query_sg=query_sg)
start = time.time()
print(start)
with Pool(processes=10) as P:
  states_done = P.map(sgijl, COIter)
print(states_done)
end = time.time()
print(end - start)

# %%
for i in COIter:
  print(i)
  sg_import_jsonlines(stateDate=i, base_path=base_path, query_sg=query_sg)

# %%

query_sg_old = """query {
  search(filter: { 
    address: {
      region: "CO"
    }
  }){
    places {
      results(first: 500 after: "") {
        pageInfo { hasNextPage, endCursor}
        edges {
          node {
            safegraph_weekly_patterns (date: "2019-01-01") {
              placekey
              parent_placekey
              location_name
              street_address
              city
              region
              postal_code
              iso_country_code
              date_range_start
              date_range_end
              raw_visit_counts
              raw_visitor_counts
              device_type
              poi_cbg
              visitor_home_cbgs
              visitor_home_aggregation
              visitor_daytime_cbgs
              visitor_country_of_origin
              distance_from_home
              median_dwell
              bucketed_dwell_times
              related_same_day_brand
              related_same_week_brand
            }
          }
        }
      }
    }
  }
}
"""


query_sg_week = """query {
  search(filter: { 
    address: {
      region: "CO"
    }
  }){
    places {
      results(first: 500 after: "") {
        pageInfo { hasNextPage, endCursor}
        edges {
          node {
            weekly_patterns (start_date: "2019-01-01" end_date: "2019-01-02") {
              placekey
              parent_placekey
              location_name
              street_address
              city
              region
              postal_code
              iso_country_code
              date_range_start
              date_range_end
              raw_visit_counts
              raw_visitor_counts
              device_type
              poi_cbg
              visitor_home_cbgs
              visitor_home_aggregation
              visitor_daytime_cbgs
              visitor_country_of_origin
              distance_from_home
              median_dwell
              bucketed_dwell_times
              related_same_day_brand
              related_same_week_brand
            }
          }
        }
      }
    }
  }
}
"""
resultsIter = client.execute(gql(query_sg_week))
resultsIterold = client.execute(gql(query_sg_old))

pageInformation = resultsIter['search']['places']['results']['pageInfo']
edgesIter = resultsIter['search']['places']['results']['edges']
    
    
pageInformation_old = resultsIterold['search']['places']['results']['pageInfo']
edgesIter_old = resultsIterold['search']['places']['results']['edges']

resultsIter = [dat.pop('node') for dat in edgesIter]
resultsIter = [dat.pop('weekly_patterns') for dat in resultsIter]

resultsIter_old = [dat.pop('node') for dat in edgesIter_old]
resultsIter_old = [dat.pop('safegraph_weekly_patterns') for dat in resultsIter_old]

print(resultsIter[1][0])
print(resultsIter_old[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- CREATE DATABASE weekly_all

# COMMAND ----------

df = spark.read.option("recursiveFileLookup","true").json("file:/dbfs/hathawayj/Colorado", schema = schema)
df.write.format("delta").saveAsTable("weekly_all.CO")
display(df)

# COMMAND ----------

# careful deletes all the previous command
shutil.rmtree(base_path)
print(base_path)


# COMMAND ----------


