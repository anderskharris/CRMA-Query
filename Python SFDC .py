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
    organizationId = config("orginizationId"),
    security_token = config("security_token"),
    domain="test",
)
print(sf.session_id, sf.sf_instance, sf.headers)

# %% This is a function to find where fields are used in recipes.

# query recipe function

def data_connections(fileUrl, Name):
    recipe_json = sf.query_more(
        f"{fileUrl}",
        identifier_is_url="true",
    )

    params = []

    for name, block in recipe_json["nodes"].items():
        if "LOAD_DATASET" in name:
            params.append(dict(block["parameters"]))

    label = []
    fields = []

    for f in params:
        if "connectedDataset" in f["dataset"]["type"]:
            label.append(f["dataset"]["label"])
            fields.append(f["fields"])

    result = dict(zip(label, fields))
    return dict({f"{Name}": result})


# field deprication search


def field_deprication(field, object):
    data_recipes = sf.query_more(
        "/services/data/v52.0/wave/recipes", identifier_is_url="true"
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

    urls = []
    names = []

    for i in df_recipes["fileUrl"]:
        urls.append(i)

    for i in df_recipes["label"]:
        names.append(i)

    recipes = dict(zip(names, urls))

    result = {}

    for key, value in recipes.items():
        try:
            result.update(data_connections(value, key))
        except:
            print("Failed to query " + key)

    recipes = []

    for key, value in result.items():
        if object in value.keys():
            if field in value[object]:
                recipes.append(key)

    return recipes


# %% This is a function to query all fields brought into recipes.

# find all fields brought into recipes from local connections


def data_connections(fileUrl, Name):
    recipe_json = sf.query_more(
        f"{fileUrl}",
        identifier_is_url="true",
    )

    params = []

    for name, block in recipe_json["nodes"].items():
        if "LOAD_DATASET" in name:
            params.append(dict(block["parameters"]))

    label = []
    fields = []

    for f in params:
        if "connectedDataset" in f["dataset"]["type"]:
            label.append(f["dataset"]["label"])
            fields.append(f["fields"])

    result = dict(zip(label, fields))
    return dict({f"{Name}": result})


# function to find fields from local connection that are used in recipes


def field_usage():
    data_recipes = sf.query_more(
        "/services/data/v52.0/wave/recipes", identifier_is_url="true"
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

    urls = []
    names = []

    for i in df_recipes["fileUrl"]:
        urls.append(i)

    for i in df_recipes["label"]:
        names.append(i)

    recipes = dict(zip(names, urls))

    result = {}

    for key, value in recipes.items():
        try:
            result.update(data_connections(value, key))
        except:
            print("Failed to query " + key)

    fields = {}

    for recipe in result.keys():
        for object, field in result[f"{recipe}"].items():
            if object in fields.keys():
                fields[f"{object}"] = [*set(fields[f"{object}"] + field)]
            else:
                fields.update({object: field})

    return fields


# %% function to find all fields in a dataset that are used

# function to find fields in a dataset

def dataset_fields(datasetapiname):
    datasets = sf.query_more(
        "/services/data/v53.0/wave/datasets/", identifier_is_url="true"
    )
    df_datasets = pd.json_normalize(datasets, record_path=["datasets"])
    try:
        DatasetsNextUrl = datasets["nextPageUrl"]
    except KeyError:
        DatasetsNextUrl = None
        print("No Next Page to Query: Datasets")
    while DatasetsNextUrl != None:
        data = sf.query_more(DatasetsNextUrl, True)
        data_df = pd.json_normalize(data, record_path=["datasets"])
        df_datasets = pd.concat([df_datasets, data_df])
        DatasetsNextUrl = data["nextPageUrl"]
    print("Datasets Queried Successfully")

    currentVersionUrl = (
        df_datasets["currentVersionUrl"]
        .loc[df_datasets["name"] == datasetapiname]
        .iloc[0]
    )

    dataset_json = sf.query_more(
        f"{currentVersionUrl}",
        identifier_is_url="true",
    )

    types_of_fields = [
        "dimensions",
        "measures",
        "derivedDimensions",
        "derivedMeasures",
    ]

    fields = []
    keys = ['recipe', 'dashboards']
    emptylist = []
    tmpkeys = dict.fromkeys(keys,emptylist)

    for type in types_of_fields:
        for i in dataset_json["xmdMain"][type]:
            for key, value in i.items():
                if "field" in key:
                    fields.append(value)
    dataset_dict = dict.fromkeys(fields, tmpkeys)
    return dataset_dict

# function to find dataset fields used in recipes

def data_connections(fileUrl, Name):
    recipe_json = sf.query_more(
        f"{fileUrl}",
        identifier_is_url="true",
    )

    params = []

    for name, block in recipe_json["nodes"].items():
        if "LOAD_DATASET" in name:
            params.append(dict(block["parameters"]))

    label = []
    fields = []

    for f in params:
        if "analyticsDataset" in f["dataset"]["type"]:
            label.append(f["dataset"]["name"])
            fields.append(f["fields"])

    result = dict(zip(label, fields))
    return dict({f"{Name}": result})


# function to find fields used in recipes from analytics datasets

def field_usage():
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

    urls = []
    names = []

    for i in df_recipes["fileUrl"]:
        urls.append(i)

    for i in df_recipes["label"]:
        names.append(i)

    recipes = dict(zip(names, urls))

    result = {}

    for key, value in recipes.items():
        try:
            result.update(data_connections(value, key))
        except:
            print("Failed to query " + key)

    return result

# function to find what dashboard datasets are used in

def dashboard_datasets(dataset):
    dashboards = sf.query_more(
        "/services/data/v52.0/wave/dashboards", identifier_is_url="true"
    )
    dashboards = pd.json_normalize(dashboards, record_path=["dashboards"])
    try:
        dashboardsNextUrl = dashboards["nextPageUrl"]
    except KeyError:
        dashboardsNextUrl = None
        print("No Next Page to Query: Dashboards")
    while dashboardsNextUrl != None:
        data = sf.query_more(dashboardsNextUrl, True)
        data_df = pd.json_normalize(data, record_path=["dashboards"])
        dashboards = pd.concat([dashboards, data_df])
        dashboardsNextUrl = data["nextPageUrl"]
    print("Dashboards Queried Successfully")

    dashboard[['datasets_parsed']] = 
    
    for i in dashboards['datasets']:
        list = []
        for x in i:
            list.append(x['name'])
            print(list)

    for key, value in tmp.items():
        for x in value:
            if dataset in x['name']:
                dashes.append(key)
    
    return(dashboards)

# function to find what fields are used in dashboards


def dashboard_fields(dataset):
    dashboard_json = sf.query_more(
        f"{fileUrl}",
        identifier_is_url="true",
    )

    params = {}
    
    for name, block in tmp["state"]["steps"].items():
        if (block)["type"] in ["aggregateflex"]:
            if 'field' in str(value["query"].values()):
                add dashboard to list
        elif (block)["type"] in ["saql"]:
            if '&#39;' + f"{field_name}" + '&#39;' in (block)["query"] or ' ' + f"{field_name}" + ' ' in (block)["query"]:
                add dashboard to list
        else:
            print("Unknown Query Type")

        label = []
        fields = []

    for f in params:
        if "analyticsDataset" in f["dataset"]["type"]:
            label.append(f["dataset"]["name"])
            fields.append(f["fields"])

    result = dict(zip(label, fields))
    return dict({f"{Name}": result})

# all the functions together

def dataset_usage(datasetapiname):
    datasets = sf.query_more(
        "/services/data/v53.0/wave/datasets/", identifier_is_url="true"
    )
    df_datasets = pd.json_normalize(datasets, record_path=["datasets"])
    try:
        DatasetsNextUrl = datasets["nextPageUrl"]
    except KeyError:
        DatasetsNextUrl = None
        print("No Next Page to Query: Datasets")
    while DatasetsNextUrl != None:
        data = sf.query_more(DatasetsNextUrl, True)
        data_df = pd.json_normalize(data, record_path=["datasets"])
        df_datasets = pd.concat([df_datasets, data_df])
        DatasetsNextUrl = data["nextPageUrl"]
    print("Datasets Queried Successfully")

    currentVersionUrl = (
        df_datasets["currentVersionUrl"]
        .loc[df_datasets["name"] == datasetapiname]
        .iloc[0]
    )

    dataset_json = sf.query_more(
        f"{currentVersionUrl}",
        identifier_is_url="true",
    )

    types_of_fields = [
        "dimensions",
        "measures",
        "derivedDimensions",
        "derivedMeasures",
    ]

    fields = []
    keys = ['recipe', 'dashboards']
    emptyset = set()
    tmpkeys = dict.fromkeys(keys,emptyset)

    for type in types_of_fields:
        for i in dataset_json["xmdMain"][type]:
            for key, value in i.items():
                if "field" in key:
                    fields.append(value)
    dataset_dict = dict.fromkeys(fields, tmpkeys)

    recipe_usage = field_usage()
    
    for field in dataset_dict.keys():
        for key, value in recipe_usage.items():
            if datasetapiname in value.keys():
                if field in value[f"{datasetapiname}"]:
                    dataset_dict[f"{field}"]['recipe'].add(f"{key}")
    
    return dataset_dict

# %%

# %%
