# %%
# https://towardsdatascience.com/connecting-to-a-graphql-api-using-python-246dda927840
import requests
import json
import pandas as pd
# %%
query = """
query {
    characters {
    results {
      name
      status
      species
      type
      gender
    }
  }
}
"""
# %%
url = 'https://rickandmortyapi.com/graphql/'
r = requests.post(url, json={'query': query})
print(r.status_code)
print(r.text)
# %%
json_data = json.loads(r.text)
# %%
df_data = json_data['data']['characters']['results']
df = pd.DataFrame(df_data)
# %%
