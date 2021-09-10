import requests
import pandas as pd
from collections import OrderedDict

def gen_cat_report(URL, PROJECT_ID, celltype_annotaion, report_name):

    dataset_outfile = "../VFB_reporting_results/CATMAID_SKID_reports/" + report_name + "_cellTypes.tsv"
    skid_outfile = "../VFB_reporting_results/CATMAID_SKID_reports/" + report_name + "_cellType_skids.tsv"
    
    # get token

    client = requests.session()
    client.get("%s" % URL)
    for key in client.cookies.keys():
        if key[:4] == 'csrf':
            csrf_key = key
    csrftoken = client.cookies[csrf_key]

    print(csrftoken)

    # Cell Types
    # pull out cell types (id) ids and names -> table and save as tsv

    call_types = {"annotated_with": celltype_annotaion, "with_annotations": False, "annotation_reference": "id"}
    celltypes = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                         data=call_types, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]

    df_types = pd.DataFrame(celltypes)
    df_types = df_types.set_index("id")
    df_types = df_types.drop("type", axis=1)
    df_types = df_types.sort_values("name")
    df_types.to_csv(dataset_outfile, sep="\t")

    # SKIDs
    # pull out neuron details for all types

    call_types["annotation_reference"] = "id"
    for celltype in celltypes:
        call_types["annotated_with"] = celltype["id"]
        neurons = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                              data=call_types, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]
        celltype["neurons"] = neurons

    # empty dataframe for details on each skid
    df_skids = pd.DataFrame(columns=['skid', 'name', 'annotation_id', 'annotation_name'])

    # populate dataframe one row at a time
    for celltype in celltypes:
        for neuron in celltype['neurons']:
            for skid in neuron['skeleton_ids']:
                row = OrderedDict()
                row['skid'] = skid  # could alternatively use neuron id - is in VFB
                row['name'] = neuron['name']
                row['annotaion_id'] = celltype['id']
                row['annotation_name'] = celltype['name']
                df_row = pd.DataFrame([row])
                df_skids = pd.concat([df_skids, df_row])

    df_skids = df_skids.sort_values(["annotation_name","skid"])
    df_skids.to_csv(skid_outfile, sep="\t", index=False)

if __name__ == '__main__':    
    gen_cat_report("https://fafb.catmaid.virtualflybrain.org",1,"11078097","FAFB_CAT")
