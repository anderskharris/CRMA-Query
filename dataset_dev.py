# %% find all fields in a dataset
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

#%%  