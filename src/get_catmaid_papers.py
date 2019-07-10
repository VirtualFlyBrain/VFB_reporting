import requests
import pandas as pd
from collections import OrderedDict

dataset_outfile = "CAT_datasets.tsv"
skid_outfile = "CAT_skids.tsv"

URL = "https://l1em.catmaid.virtualflybrain.org"
PROJECT_ID = 1

# get token

client = requests.session()
client.get("%s" % URL)
for key in client.cookies.keys():
    if key[:4] == 'csrf':
        csrf_key = key
csrftoken = client.cookies[csrf_key]

print(csrftoken)

# PAPERs
# pull out paper ids and names -> table and save as tsv

call_papers = {"annotated_with": "papers", "with_annotations": False, "annotation_reference": "name"}
papers = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                     data=call_papers, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]

df_papers = pd.DataFrame(papers)
df_papers = df_papers.set_index("id")
df_papers = df_papers.drop("type", axis=1)
df_papers.to_csv(dataset_outfile, sep="\t")

# SKIDs
# pull out neuron details for all papers

call_papers["annotation_reference"] = "id"
for paper in papers:
    call_papers["annotated_with"] = paper["id"]
    neurons = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                          data=call_papers, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]
    paper["neurons"] = neurons

# empty dataframe for details on each skid
df_skids = pd.DataFrame(columns=['skid', 'name', 'paper_id', 'paper_name'])

# populate dataframe one row at a time
for paper in papers:
    for neuron in paper['neurons']:
        for skid in neuron['skeleton_ids']:
            row = OrderedDict()
            row['skid'] = skid  # could alternatively use neuron id - is in VFB
            row['name'] = neuron['name']
            row['paper_id'] = paper['id']
            row['paper_name'] = paper['name']
            df_row = pd.DataFrame([row])
            df_skids = pd.concat([df_skids, df_row])


df_skids.to_csv(skid_outfile, sep="\t", index=False)
