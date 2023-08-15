# %% function to get all inputs from recipes 
def recipe_inputs(fileUrl, Name):
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
    type = []

    for f in params:
        if "connectedDataset" in f["dataset"]["type"] or "analyticsDataset" in f["dataset"]["type"]:
            label.append(f["dataset"]["label"])
            fields.append(f["fields"])

    result = dict(zip(label, fields))
    return dict({f"{Name}": result})

# %% function to get all outputs from recipes

# %% function to get call all recipes and compile end result
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