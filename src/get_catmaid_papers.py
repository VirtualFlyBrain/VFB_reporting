import requests
import pandas as pd
from collections import OrderedDict

# functions for getting paper and skid details from CATMAID
# reports are generated by comparison.py

def gen_cat_paper_report(URL, PROJECT_ID, paper_annotaion, report_name):
    """Gets IDs and names of papers in CATMAID and returns a dataframe."""

    # FOR SAVING OUTPUT FILE IF DESIRED
    # dataset_outfile = "../VFB_reporting_results/" + report_name + "_datasets.tsv"

    # get token

    client = requests.session()
    client.get("%s" % URL)
    for key in client.cookies.keys():
        if key[:4] == 'csrf':
            csrf_key = key
    csrftoken = client.cookies[csrf_key]

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


def gen_cat_skid_report(URL, PROJECT_ID, paper_annotaion, report_name):
    """Gets IDs and names of papers in CATMAID, then skids for the neurons in each paper and returns a dataframe.
    NB. each skid may feature in multiple papers."""

    # FOR SAVING OUTPUT FILE IF DESIRED
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
        neurons = []
        try:
            neurons = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                              data=call_papers, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]
        except:
            print("An exception occurred getting skids from " + str(URL) + " annotated with paper id:" + str(paper["id"]))
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

def gen_cat_skid_report_officialnames(URL, PROJECT_ID, paper_annotaion, name_annotation, report_name):
    """Gets IDs and names of papers in CATMAID, then skids for the neurons in each paper and returns a dataframe.
    This version collects the official names from annotations that are annotaed with a name_annotation 
    NB. each skid may feature in multiple papers."""

    # FOR SAVING OUTPUT FILE IF DESIRED
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

    # NAMES
    # pull out official names

    call_names = {"annotated_with": name_annotation, "with_annotations": False, "annotation_reference": "name"}
    names = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                         data=call_names, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]

    # get neuron info for each paper
    call_papers["annotation_reference"] = "id"
    call_papers["with_annotations"] = True
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
                for annotation in neuron["annotations"]:
                  for name in names:
                    if annotation["id"] == name["id"]:
                      row['name'] = name["name"]
                      break
                df_row = pd.DataFrame([row])
                df_skids = pd.concat([df_skids, df_row])

    df_skids = df_skids.sort_values(["paper_name", "skid"])
    # df_skids.to_csv(skid_outfile, sep="\t", index=False)  # FOR SAVING OUTPUT FILE IF DESIRED
    return df_skids

if __name__ == '__main__':
    gen_cat_skid_report_officialnames("https://l1em.catmaid.virtualflybrain.org", 1, "papers", "neuron name,MB nomenclature", "L1EM").to_csv("../VFB_reporting_results/EM_CATMAID_L1_skids.tsv", sep="\t", index=False)
    gen_cat_skid_report_officialnames("https://fafb.catmaid.virtualflybrain.org", 1, "Published", "neuron name", "FAFB").to_csv("../VFB_reporting_results/EM_CATMAID_FAFB_skids.tsv", sep="\t", index=False)
    gen_cat_skid_report("https://vnc1.catmaid.virtualflybrain.org", 1, "publication", "VNC1").to_csv("../VFB_reporting_results/EM_CATMAID_VNC1_skids.tsv", sep="\t", index=False)
