# We use three Python packages to get data from SafeGraph - `gql`, `requests`, and `safegraphql`. We elected to signal the start of each type of API request with the package imports spread throughout the script.

# We will focus on the `gql` or `requests` examples for our work. We will stay in `gql` and highly recommend that you don't use `graphql`.

# %%
# import sys
# !{sys.executable} -m pip install --pre gql 
# !{sys.executable} -m pip install requests-toolbelt
# %%
# https://docs.safegraph.com/reference#places-api-overview-new
# https://stackoverflow.com/questions/56856005/how-to-set-environment-variable-in-databricks/56863551
import pandas as pd
import json

import os
from dotenv import load_dotenv

load_dotenv()
sfkey = os.environ.get("SAFEGRAPH_KEY")

# %%
url = 'https://api.safegraph.com/v2/graphql'
query = """
query {
  search(
      filter: {
        address: { 
            city: "San Francisco", 
            region: "CA" 
        }
    }
    ) {
    places {
      results(first: 25 after: "") {
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

# old safegraph_weekly_patterns()
query2 = """query {
  search(filter: { 
    naics_code: 813110,
    address: {
      region: "UT"
    }
  }){
    places {
      results(first: 25 after: "") {
        pageInfo { hasNextPage, endCursor}
        edges {
          node {
            safegraph_weekly_patterns (date: "2021-07-12") {
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

# uses the new weekly_patterns () function.
query3 = """query {
  search(
      filter: {
        address: { 
            city: "San Francisco", 
            region: "CA" 
        }
    }
    ){
    places {
      results(first: 25 after: "") {
        pageInfo { hasNextPage, endCursor}
        edges {
          node {
            weekly_patterns (start_date: "2019-01-07" end_date: "2019-01-13") {
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
              poi_cbg
              distance_from_home
              median_dwell
            }
          }
        }
      }
    }
  }
}
"""

# %%
# Using the requests package
import requests
r = requests.post(
    url,
    json={'query': query},
    headers = {'Content-Type': 'application/json', 'apikey':sfkey})

# %%
print(r.status_code)
print(r.text)
json_data = json.loads(r.text)
df_data = json_data['data']['search']['places']['results']['edges']
print(df_data)

# %%
pract = df_data.copy()

pd.json_normalize(pract)
# %%

# https://gql.readthedocs.io/en/v3.0.0a6/
# https://github.com/graphql-python/gql
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

# Select your transport with a defined url endpoint
transport = RequestsHTTPTransport(
    url=url,
    verify=True,
    retries=3,
    headers={'Content-Type': 'application/json', 'apikey': sfkey})

client = Client(transport=transport, fetch_schema_from_transport=True)


# %%
results = client.execute(gql(query))
results2 = client.execute(gql(query2))
results3 = client.execute(gql(query3))

# %%
edges = results['search']['places']['results']['edges']
resultsNorm = [dat.pop('node') for dat in edges]
resultsNorm = [dat.pop('safegraph_core') for dat in resultsNorm]

dat = pd.json_normalize(resultsNorm)

# %%
edges2 = results2['search']['places']['results']['edges']
resultsNorm = [dat.pop('node') for dat in edges2]
resultsNorm = [dat.pop('safegraph_weekly_patterns') for dat in resultsNorm]

dat = pd.json_normalize(resultsNorm)


# %%
edges3 = results3['search']['places']['results']['edges']
resultsNorm = [dat.pop('node') for dat in edges3]
resultsNorm = [dat.pop('weekly_patterns') for dat in resultsNorm]
resultsNorm_flat = [dat[0] for dat in resultsNorm if dat is not None]


dat = pd.json_normalize(resultsNorm_flat)


# %%
# Don't use
# https://pypi.org/project/safegraphQL/
# import sys
# !{sys.executable} -m pip install safegraphQL

# %%
# https://pypi.org/project/safegraphQL/
# https://github.com/echong-SG/API-python-client-MKilic
# import safegraphql.client as sgql
# sgql_client = sgql.HTTP_Client(apikey = sfkey)

# # %%
# pks = [
#     'zzw-222@8fy-fjg-b8v', # Disney World 
#     'zzw-222@5z6-3h9-tsq'  # LAX
# ]
# cols = [
#     'location_name',
#     'street_address',
#     'city',
#     'region',
#     'postal_code',
#     'iso_country_code'
# ]

# sgql_client.lookup(product = 'core', placekeys = pks, columns = cols)
# # %%
# sgql_client.lookup(product = 'core', placekeys = pks, columns = "*")

# # %%
# geo = sgql_client.lookup(product = 'geometry', placekeys = pks, columns = '*')
# patterns = sgql_client.lookup(product = 'monthly_patterns', placekeys = pks, columns = '*')

# # %%
# watterns = sgql_client.lookup(product = 'weekly_patterns', placekeys = pk, columns = '*')

# # %%
# ## weekly patterns
# dates = ['2019-06-15', '2019-06-16', '2021-05-23', '2018-10-23']

# sgql_client.lookup(
#     product = 'weekly_patterns', 
#     placekeys = pks, 
#     date = dates, 
#     columns = ['placekey', 'location_name', 'date_range_start', 'date_range_end', 'raw_visit_counts']
# )
# # %%
# dates = {'date_range_start': '2019-04-10', 'date_range_end': '2019-06-05'}

# watterns = sgql_client.lookup(
#     product = 'weekly_patterns', 
#     placekeys = pks, 
#     date = dates, 
#     columns = ['placekey', 'location_name', 'date_range_start', 'date_range_end', 'raw_visit_counts']
# )

# core = sgql_client.lookup(product = 'core', placekeys = pks, columns = ['placekey', 'location_name', 'naics_code', 'top_category', 'sub_category'])
# geo = sgql_client.lookup(product = 'geometry', placekeys = pks, columns = ['placekey', 'polygon_class', 'enclosed'])

# # %%
# merged = sgql_client.sg_merge(datasets = [core, geo, watterns])
# # %%
# # look-up by name
# # https://github.com/echong-SG/API-python-client-MKilic#lookup_by_name