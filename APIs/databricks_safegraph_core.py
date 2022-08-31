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
query_core = """
query {
  search(filter:{
    address: {
         region: "CO"}
    }) {
    places {
      results(first: 500 after: "") {
      pageInfo {hasNextPage, endCursor}
      edges {
        node {
          safegraph_core {
          	placekey
            latitude
            longitude
            street_address
            city
            region
            postal_code
            parent_placekey
            location_name
            naics_code
            opened_on
            closed_on
            }
        	}
      	}
    	}
  	}
	}
}
"""

# %%

def sg_import_jsonlines(query_sg, base_path, naic_filter = None, printquery=False, nextPageStart=None):

  if naic_filter == None:
    filter_text = ''
  else:
    # "naics_code: 813110,"
    filter_text = naic_filter[0] + ": " + naic_filter[1] + ',' 
  
  try:
    os.mkdir(base_path + "/core")
  except:
    print("Made earlier")

  resultsIter = client.execute(gql(query_sg))

  pageInformation = resultsIter['search']['places']['results']['pageInfo']
      

  if printquery:
    print(query_state)

  pagestep = 0

  while (pageInformation['hasNextPage'] or (pagestep == 0)):
  
    if pagestep != 0:
      nextString = 'after: "' + pageInformation['endCursor'] + '"'
      queryIter = query_sg.replace('after: ""', nextString)
      resultsIter = client.execute(gql(queryIter))
      
    else:
      print(str(pagestep) + " : First Call")
  
    pageInformation = resultsIter['search']['places']['results']['pageInfo']
    edgesIter = resultsIter['search']['places']['results']['edges']
    resultsIter = [dat.pop('node') for dat in edgesIter]
    resultsIter = [dat.pop('safegraph_core') for dat in resultsIter]

    pathString = base_path + "/" + 'json_' + str(pagestep) + '.jl'
    
    
    with jsonlines.open(pathString, 'w') as writer:
      writer.write_all(resultsIter)
      writer.close()
    print(str(pagestep) + "stuff: " + pageInformation['endCursor'] + " , ", end =" ")
    pagestep += 1
  return (pageInformation['endCursor'], pagestep)
# %%
sg_import_jsonlines(query_sg = query_core, base_path = "Colorado", naic_filter = None, printquery=False, nextPageStart=None)
# %%
