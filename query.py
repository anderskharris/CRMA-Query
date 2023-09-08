# %%
# import packages
import yaml
from simple_salesforce import Salesforce
import json
import pandas as pd
import os

# %%
# authenticate org
with open("config.yaml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

with open("secrets.yaml", "r") as f:
    secrets = yaml.load(f, Loader=yaml.FullLoader)

sf = Salesforce(
    username = secrets["username"],
    password = secrets["password"],
    organizationId = config["organizationId"],
    security_token = secrets["token"],
    domain="test",
)
print(sf.session_id, sf.sf_instance, sf.headers)

# %%
data_recipes = sf.query_more(
    "/services/data/v56.0/wave/recipes", identifier_is_url="true"
   )
df_recipes = pd.json_normalize(data_recipes, record_path=["recipes"])
try:
    recipesNextUrl = data_recipes["nextPageUrl"]
except KeyError:
    recipesNextUrl = None
    print("No Next Page to Query: Recipes")
while recipesNextUrl != None:
    data = sf.query_more(recipesNextUrl, True)
    data_df = pd.json_normalize(data, record_path=["recipes"])
    df_recipes = pd.concat([df_recipes, data_df])
    recipesNextUrl = data["nextPageUrl"]
print("Recipes Queried Successfully")
# %%
