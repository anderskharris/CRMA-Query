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