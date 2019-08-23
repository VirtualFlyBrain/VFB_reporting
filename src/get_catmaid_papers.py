import requests
import pandas as pd
from collections import OrderedDict


def gen_cat_paper_report(URL, PROJECT_ID, paper_annotaion, report_name):
    """Gets IDs and names of papers in CATMAID and returns a dataframe."""

    # FOR SAVING OUTPUT FILE IF DESIRED
    # dataset_outfile = "../VFB_reporting_results/" + report_name + "_datasets.tsv"
    # skid_outfile = "../VFB_reporting_results/" + report_name + "_skids.tsv"
    
    # get token

    client = requests.session()
    client.get("%s" % URL)
    for key in client.cookies.keys():
        if key[:4] == 'csrf':
            csrf_key = key
    csrftoken = client.cookies[csrf_key]

    # print(csrftoken)

    # PAPERs
    # pull out paper ids and names

    call_papers = {"annotated_with": paper_annotaion, "with_annotations": False, "annotation_reference": "name"}
    papers = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                         data=call_papers, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]

    df_papers = pd.DataFrame(papers)
    df_papers = df_papers.set_index("id")
    df_papers = df_papers.drop("type", axis=1)
    df_papers = df_papers.sort_values("name")
    # df_papers.to_csv(dataset_outfile, sep="\t")  # FOR SAVING OUTPUT FILE IF DESIRED
    return df_papers

    # SKIDs
    # pull out neuron details for all papers


def gen_cat_skid_report(URL, PROJECT_ID, paper_annotaion, report_name):
    """Gets IDs and names of papers in CATMAID, then skids for the neurons in each paper and returns a dataframe.

    NB. each skid may feature in multiple papers."""

    # FOR SAVING OUTPUT FILE IF DESIRED
    # dataset_outfile = "../VFB_reporting_results/" + report_name + "_datasets.tsv"
    # skid_outfile = "../VFB_reporting_results/" + report_name + "_skids.tsv"

    # get token

    client = requests.session()
    client.get("%s" % URL)
    for key in client.cookies.keys():
        if key[:4] == 'csrf':
            csrf_key = key
    csrftoken = client.cookies[csrf_key]

    # PAPERs

    # pull out paper ids and names from CATMAID
    call_papers = {"annotated_with": paper_annotaion, "with_annotations": False, "annotation_reference": "name"}
    papers = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                         data=call_papers, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]

    # get neuron info for each paper
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

    df_skids = df_skids.sort_values(["paper_name", "skid"])
    # df_skids.to_csv(skid_outfile, sep="\t", index=False)  # FOR SAVING OUTPUT FILE IF DESIRED
    return df_skids

    
gen_cat_paper_report("https://l1em.catmaid.virtualflybrain.org", 1, "papers", "L1_CAT")
gen_cat_paper_report("https://fafb.catmaid.virtualflybrain.org", 1, "Published", "FAFB_CAT")
