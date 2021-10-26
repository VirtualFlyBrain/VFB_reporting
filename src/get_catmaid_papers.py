import requests
import json
import pandas as pd
from collections import OrderedDict

# functions for getting paper and skid details from CATMAID
# reports are generated by comparison.py


def gen_cat_paper_report(URL, PROJECT_ID, paper_annotation, report=False):
    """Gets IDs and names of papers in CATMAID and returns a dataframe."""

    print("Getting Papers from %s ..." % URL)
    
    # get token
    client = requests.session()
    client.get("%s" % URL)
    for key in client.cookies.keys():
        if key[:4] == 'csrf':
            csrf_key = key
    csrftoken = client.cookies[csrf_key]

    # PAPERs
    # pull out paper ids and names

    call_papers = {"annotated_with": paper_annotation, "with_annotations": False, "annotation_reference": "name"}
    papers = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                         data=call_papers, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]

    df_papers = pd.DataFrame(papers)
    df_papers = df_papers.set_index("id")
    df_papers = df_papers.drop("type", axis=1)
    df_papers = df_papers.sort_values("name")
    if report:
        dataset_outfile = ("../VFB_reporting_results/CATMAID_SKID_reports/" + report + "_datasets.tsv")
        df_papers.to_csv(dataset_outfile, sep="\t")

    return df_papers


def gen_cat_skid_report(URL, PROJECT_ID, paper_annotation, report=False):
    """Gets IDs and names of papers in CATMAID, then skids for the neurons in each paper and returns a dataframe.
    NB. each skid may feature in multiple papers."""

    # get token
    client = requests.session()
    client.get("%s" % URL)
    for key in client.cookies.keys():
        if key[:4] == 'csrf':
            csrf_key = key
    csrftoken = client.cookies[csrf_key]

    # PAPERs

    # pull out paper ids and names from CATMAID
    call_papers = {"annotated_with": paper_annotation, "with_annotations": False, "annotation_reference": "name"}
    papers = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                         data=call_papers,
                         headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]

    # get neuron info for each paper
    call_papers["annotation_reference"] = "id"
    call_papers["with_annotations"] = True
    for paper in papers:
        call_papers["annotated_with"] = paper["id"]
        neurons = []
        try:
            neurons = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                                  data=call_papers,
                                  headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]
        except:
            print("An exception occurred getting skids from " + str(URL) +
                  " annotated with paper id:" + str(paper["id"]))
        paper["neurons"] = neurons

    # empty dataframe for details on each skid
    df_skids = pd.DataFrame(columns=['skid', 'name', 'paper_id', 'paper_name', 'annotations'])

    # populate dataframe one row at a time
    for paper in papers:
        for neuron in paper['neurons']:
            if neuron['type'] == 'neuron':
                for skid in neuron['skeleton_ids']:
                    row = OrderedDict()
                    row['skid'] = skid  # could alternatively use neuron id - is in VFB
                    row['name'] = neuron['name']
                    row['paper_id'] = paper['id']
                    row['paper_name'] = paper['name']
                    row['annotations'] = ""
                    for annotation in neuron['annotations']:
                        if not row['annotations'] == "":
                            row['annotations'] += ", "
                        row['annotations'] += annotation['name'] + " (" + annotation['id'] + ")"
                    df_row = pd.DataFrame([row])
                    df_skids = pd.concat([df_skids, df_row])

    df_skids = df_skids.sort_values(["paper_name", "skid"])

    if report:
        skid_outfile = ("../VFB_reporting_results/CATMAID_SKID_reports/" + report + "_all_skids.tsv")
        df_skids.to_csv(skid_outfile, sep="\t", index=False)
    return df_skids


def gen_cat_skid_report_officialnames(URL, PROJECT_ID, paper_annotation, name_annotations, report=False):
    """Gets IDs and names of papers in CATMAID, then skids for the neurons in each paper and returns a dataframe.
    This version collects the official names from annotations that are annotated with one of the name_annotations 
    NB. each skid may feature in multiple papers."""

    # get token

    client = requests.session()
    client.get("%s" % URL)
    for key in client.cookies.keys():
        if key[:4] == 'csrf':
            csrf_key = key
    csrftoken = client.cookies[csrf_key]

    # PAPERS
    # pull out paper ids and names from CATMAID
    call_papers = {"annotated_with": paper_annotation, "with_annotations": False, "annotation_reference": "name"}
    papers = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                         data=call_papers, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()["entities"]

    # NAMES
    # pull out official names
    names_list = []
    for name_annotation in name_annotations:
        try:
            call_names = {"annotated_with": name_annotation, "with_annotations": False, "annotation_reference": "name"}
            results = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                                  data=call_names, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()
            if "entities" in results.keys():
                names_list.append(results["entities"])
            else:
                print("entities missing from result:")
                print(URL)
                print(name_annotation)
                print(json.dumps(results, indent=4))
        except:
            print(URL)
            print("Error finding annotation for: " + name_annotation)
                
    # get neuron info for each paper
    call_papers["with_annotations"] = True
    call_papers["annotation_reference"] = "id"
    for paper in papers:
        call_papers["annotated_with"] = paper["id"]
        try:
            neurons = client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID),
                                  data=call_papers, headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()
            if "entities" in neurons.keys():
                paper["neurons"] = neurons["entities"]
            else:
                print(URL)
                print("Error handling finding neurons for paper: " + paper["name"])
                print("Json returned: \n" + json.dumps(neurons, indent=4))
        except:
            print(URL)
            print("Error handling finding neurons for paper: " + paper["name"])
            print("Json returned: \n" +
                  json.dumps(client.post("%s/%d/annotations/query-targets" % (URL, PROJECT_ID), data=call_papers,
                                         headers={"Referer": URL, "X-CSRFToken": csrftoken}).json(), indent=4))

    # empty dataframe for details on each skid
    df_skids = pd.DataFrame(columns=['skid', 'name', 'paper_id', 'paper_name'])

    # populate dataframe one row at a time
    for paper in papers:
        for neuron in paper['neurons']:
            if 'skeleton_ids' in neuron.keys():
                for skid in neuron['skeleton_ids']:
                    row = OrderedDict()
                    row['skid'] = skid  # could alternatively use neuron id - is in VFB
                    row['name'] = neuron['name']
                    row['paper_id'] = paper['id']
                    row['paper_name'] = paper['name']
                    for annotation in neuron["annotations"]:
                        for names in names_list:
                            for name in names:
                                if annotation["id"] == name["id"]:
                                    if 'synonyms' in row.keys():
                                        row['synonyms'] = row['synonyms'] + '|' + row['name']
                                    else:
                                        row['synonyms'] = row['name']
                                    row['name'] = name["name"]
                                    break
                    df_row = pd.DataFrame([row])
                    df_skids = pd.concat([df_skids, df_row])
            else:
                print("skeleton_ids missing from:")
                print(json.dumps(neuron, indent=4))

    df_skids = df_skids.sort_values(["paper_name", "skid"])
    if report:
        skid_outfile = ("../VFB_reporting_results/CATMAID_SKID_reports/" +
                        report + "_all_skids_officialnames.tsv")
        df_skids.to_csv(skid_outfile, sep="\t", index=False)
    else:
        print("ERROR: No report created for " + URL)
    return df_skids


if __name__ == '__main__':
    # generate skid reports when this is run as a script

    # variables for generating reports
    # dict of sources and project IDs for larval datasets
    larval_sources = {'l1em': 1, 'abd1.5': 1, 'iav-robo': 1, 'iav-tnt': 2, 'l3vnc': 2}
    larval_reports = [["https://" + s + ".catmaid.virtualflybrain.org", larval_sources[s], "papers",
                       ["neuron name", "MB nomenclature"], s.upper()]
                      for s in larval_sources.keys()]

    FAFB = ["https://fafb.catmaid.virtualflybrain.org", 1, "Published", ["neuron name"], "FAFB"]
    FANC1 = ["https://fanc.catmaid.virtualflybrain.org", 1, "publication", ["neuron name"], "FANC1"]
    FANC2 = ["https://fanc.catmaid.virtualflybrain.org", 2, "publication", ["neuron name"], "FANC2"]

    # make reports
    for r in larval_reports:
        gen_cat_skid_report_officialnames(*r)
    gen_cat_skid_report_officialnames(*FAFB)
    gen_cat_skid_report_officialnames(*FANC1)
    gen_cat_skid_report_officialnames(*FANC2)
