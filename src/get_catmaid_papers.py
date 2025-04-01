import requests
import json
import pandas as pd
import datetime
from collections import OrderedDict
import sys
import os

# Try importing Neo4j tools with proper error handling
try:
    from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    log_error("Neo4j tools not available. Missing links report functionality will be limited.")

# functions for getting paper and skid details from CATMAID
# reports are generated by comparison.py

# Define server-specific annotation mappings
ANNOTATION_MAPPINGS = {
    "fafb.catmaid.virtualflybrain.org": ["neuron name"],
    "l1em.catmaid.virtualflybrain.org": ["cell name", "neuron name", "MB nomenclature"],
    "abd1.5.catmaid.virtualflybrain.org": ["cell name", "neuron name", "MB nomenclature"],
    "iav-robo.catmaid.virtualflybrain.org": ["cell name", "neuron name", "MB nomenclature"],
    "iav-tnt.catmaid.virtualflybrain.org": ["cell name", "neuron name", "MB nomenclature"],
    "l3vnc.catmaid.virtualflybrain.org": ["cell name", "neuron name", "MB nomenclature"],
    "fanc.catmaid.virtualflybrain.org": ["cell type", "neuron name"],
    "radagast.hms.harvard.edu": ["cell type", "neuron name", "official name"]
}

def get_annotation_tags(url):
    """Get appropriate annotation tags for a specific CATMAID server"""
    domain = url.split("//")[1].split("/")[0]
    return ANNOTATION_MAPPINGS.get(domain, ["neuron name"])  # Default to neuron name


def log_error(message, details=None):
    """Helper function to log errors consistently"""
    print(f"ERROR: {message}")
    if details:
        print(f"Details: {json.dumps(details, indent=4) if isinstance(details, dict) else str(details)}")


def log_info(message):
    """Helper function to log informational messages consistently"""
    print(f"INFO: {message}")


def gen_cat_paper_report(URL, PROJECT_ID, paper_annotation, report=False):
    """Gets IDs and names of papers in CATMAID and returns a dataframe."""
    print(f"Getting Papers from {URL} ...")
    
    try:
        # get token
        client = requests.session()
        client.get(f"{URL}")
        csrf_key = next((key for key in client.cookies.keys() if key.startswith('csrf')), None)
        if not csrf_key:
            log_error(f"No CSRF token found for {URL}")
            return pd.DataFrame()
        csrftoken = client.cookies[csrf_key]

        # Pull out paper ids and names
        call_papers = {"annotated_with": paper_annotation, "with_annotations": False, "annotation_reference": "name"}
        response = client.post(f"{URL}/{PROJECT_ID}/annotations/query-targets",
                             data=call_papers, 
                             headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()
        
        if "entities" not in response:
            log_error(f"No entities found in response for {URL}", response)
            return pd.DataFrame()

        papers = response["entities"]
        df_papers = pd.DataFrame(papers)
        
        if df_papers.empty:
            log_error(f"No papers found for {URL}")
            return df_papers

        df_papers = df_papers.set_index("id")
        df_papers = df_papers.drop("type", axis=1)
        df_papers = df_papers.sort_values("name")

        if report:
            try:
                dataset_outfile = f"../VFB_reporting_results/CATMAID_SKID_reports/{report}_datasets.tsv"
                df_papers.to_csv(dataset_outfile, sep="\t")
            except Exception as e:
                log_error(f"Failed to save report for {URL}", str(e))

        return df_papers

    except Exception as e:
        log_error(f"Failed to process {URL}", str(e))
        return pd.DataFrame()


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
                        row['annotations'] += annotation['name'] + " (" + str(annotation['id']) + ")"
                    df_row = pd.DataFrame([row])
                    df_skids = pd.concat([df_skids, df_row])

    df_skids = df_skids.sort_values(["paper_name", "skid"])

    if report:
        skid_outfile = ("../VFB_reporting_results/CATMAID_SKID_reports/" + report + "_all_skids.tsv")
        df_skids.to_csv(skid_outfile, sep="\t", index=False)
    return df_skids


def gen_cat_skid_report_officialnames(URL, PROJECT_ID, paper_annotation, name_annotations=None, report=False):
    """Gets IDs and names of papers in CATMAID, then skids for the neurons in each paper.
    Collects official names from annotations that are annotated with one of the name_annotations."""
    
    print(f"Processing {URL}")
    
    try:
        # If name_annotations is not provided, use server-specific mappings
        if name_annotations is None or len(name_annotations) == 0:
            name_annotations = get_annotation_tags(URL)
            log_info(f"Using server-specific annotation tags: {name_annotations}")
        
        # get token
        client = requests.session()
        client.get(f"{URL}")
        csrf_key = next((key for key in client.cookies.keys() if key.startswith('csrf')), None)
        if not csrf_key:
            log_error(f"No CSRF token found for {URL}")
            return pd.DataFrame()
        csrftoken = client.cookies[csrf_key]

        # Get papers
        call_papers = {"annotated_with": paper_annotation, "with_annotations": False, "annotation_reference": "name"}
        response = client.post(f"{URL}/{PROJECT_ID}/annotations/query-targets",
                             data=call_papers, 
                             headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()
        
        if "entities" not in response:
            log_error(f"No paper entities found in response for {URL}", response)
            papers = []
        else:
            papers = response["entities"]
            log_info(f"Found {len(papers)} papers for {URL}")

        # Get official names
        names_list = []
        for name_annotation in name_annotations:
            try:
                call_names = {"annotated_with": name_annotation, "with_annotations": False, "annotation_reference": "name"}
                results = client.post(f"{URL}/{PROJECT_ID}/annotations/query-targets",
                                   data=call_names, 
                                   headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()
                
                if "entities" in results:
                    names_list.append(results["entities"])
                    log_info(f"Found {len(results['entities'])} annotations with '{name_annotation}' for {URL}")
                else:
                    log_error(f"Missing entities for annotation {name_annotation} at {URL}", results)
            except Exception as e:
                log_error(f"Failed to get annotation {name_annotation} at {URL}", str(e))
                continue

        # Process neurons for each paper
        df_skids = pd.DataFrame(columns=['skid', 'name', 'paper_id', 'paper_name', 'annotations', 'synonyms'])
        
        for paper in papers:
            try:
                call_papers.update({
                    "with_annotations": True,
                    "annotation_reference": "id",
                    "annotated_with": paper["id"]
                })
                
                neurons_response = client.post(f"{URL}/{PROJECT_ID}/annotations/query-targets",
                                            data=call_papers,
                                            headers={"Referer": URL, "X-CSRFToken": csrftoken}).json()
                
                if "entities" not in neurons_response:
                    log_error(f"No neuron entities found for paper {paper['name']} at {URL}", neurons_response)
                    continue
                    
                neurons = neurons_response["entities"]
                log_info(f"Processing {len(neurons)} entities for paper {paper['name']}")
                
                for neuron in neurons:
                    # Check if neuron is actually a neuron and has skeleton IDs
                    if neuron.get('type') != 'neuron':
                        log_info(f"Skipping non-neuron entity {neuron.get('name', 'unknown')}")
                        continue
                        
                    if 'skeleton_ids' not in neuron or not neuron['skeleton_ids']:
                        log_info(f"Skipping neuron without skeleton_ids: {neuron.get('name', 'unknown')}")
                        continue
                        
                    for skid in neuron['skeleton_ids']:
                        row = OrderedDict({
                            'skid': skid,
                            'name': neuron['name'],
                            'paper_id': paper['id'],
                            'paper_name': paper['name'],
                            'annotations': "",
                            'synonyms': ""
                        })
                        
                        # Process annotations and names
                        for annotation in neuron.get("annotations", []):
                            # Handle official names
                            for names in names_list:
                                for name in names:
                                    if annotation["id"] == name["id"]:
                                        if row['synonyms']:
                                            row['synonyms'] += f"|{row['name']}"
                                        else:
                                            row['synonyms'] = row['name']
                                        row['name'] = name["name"]
                                        break
                            
                            # Build annotations string
                            if row['annotations']:
                                row['annotations'] += ", "
                            row['annotations'] += f"{annotation['name']} ({annotation['id']})"
                        
                        df_skids = pd.concat([df_skids, pd.DataFrame([row])], ignore_index=True)
                        
            except Exception as e:
                log_error(f"Failed to process paper {paper.get('name', 'unknown')} at {URL}", str(e))
                continue

        df_skids = df_skids.sort_values(["paper_name", "skid"])
        
        if report:
            try:
                outfile = f"../VFB_reporting_results/CATMAID_SKID_reports/{report}_all_skids_officialnames.tsv"
                df_skids.to_csv(outfile, sep="\t", index=False)
                log_info(f"Saved report to {outfile}")
            except Exception as e:
                log_error(f"Failed to save report for {URL}", str(e))
        
        return df_skids

    except Exception as e:
        log_error(f"Failed to process {URL}", str(e))
        return pd.DataFrame()


def gen_missing_links_report(URL, PROJECT_ID, paper_annotation, report=False):
    """Generate a report of neurons that exist in VFB but aren't linked properly to CATMAID datasets.
    Outputs Cypher queries needed to add the missing links."""
    
    if not NEO4J_AVAILABLE:
        log_error("Neo4j tools not available. Cannot generate missing links report.")
        return []
    
    log_info(f"Generating missing links report for {URL}, project {PROJECT_ID}, annotation '{paper_annotation}'")
    
    # First get all skids from CATMAID for this dataset
    cat_skids = gen_cat_skid_report_officialnames(URL, PROJECT_ID, paper_annotation, report=report)
    
    if cat_skids.empty:
        log_error(f"No SKIDs found for {URL}. Cannot generate missing links report.")
        return []
    
    log_info(f"Found {len(cat_skids)} SKIDs for analysis")
    
    # Use the domain to identify the CATMAID instance
    domain = URL.split("//")[1].split("/")[0]
    site_short = domain.split(".")[0] if "catmaid.virtualflybrain.org" in domain else "catmaid_vnc"
    
    # Connect to Neo4j
    try:
        nc = neo4j_connect('http://kb.virtualflybrain.org', 'neo4j', 'vfb')
        log_info("Successfully connected to Neo4j database")
    except Exception as e:
        log_error(f"Failed to connect to Neo4j database: {str(e)}")
        return []
    
    # Get the dataset ID for the paper
    paper_ids = cat_skids['paper_id'].unique().tolist()
    cypher_queries = []
    total_missing = 0
    
    log_info(f"Analyzing {len(paper_ids)} papers")
    
    # First, get a mapping of all SKIDs to VFB neurons for this CATMAID instance
    site_query = f"""
    MATCH (i:Individual)-[skid:database_cross_reference]->(s:Site)
    WHERE s.short_form starts with 'catmaid_{site_short.lower()}'
    RETURN i.short_form as vfb_id, i.label as vfb_label, skid.accession[0] as skid
    """
    
    try:
        results = nc.commit_list([site_query])
        skid_to_neuron_map = {}
        for record in results_2_dict_list(results):
            skid = int(record['skid'])  # Convert to int for consistent comparison
            skid_to_neuron_map[skid] = {
                'vfb_id': record['vfb_id'],
                'vfb_label': record['vfb_label']
            }
        log_info(f"Found {len(skid_to_neuron_map)} neurons with SKIDs in VFB for site {site_short}")
    except Exception as e:
        log_error(f"Failed to query all neurons with SKIDs: {str(e)}")
        return []
    
    # Process each paper
    for paper_id in paper_ids:
        paper_name = cat_skids[cat_skids['paper_id'] == paper_id]['paper_name'].iloc[0]
        paper_skids = [int(skid) for skid in cat_skids[cat_skids['paper_id'] == paper_id]['skid'].tolist()]
        
        log_info(f"Checking paper ID {paper_id} ({paper_name}) with {len(paper_skids)} SKIDs")
        
        if not paper_skids:
            log_info(f"No SKIDs for paper {paper_id}. Skipping.")
            continue
        
        # First get the dataset ID for this specific paper
        ds_query = f"""
        MATCH (ds:DataSet)-[r:database_cross_reference]->(api:API) 
        WHERE api.short_form ends with '_catmaid_api' AND r.accession[0] = '{paper_id}' 
        RETURN ds.short_form as ds_id
        """
        
        ds_results = nc.commit_list([ds_query])
        ds_info = results_2_dict_list(ds_results)
        
        if not ds_info:
            log_error(f"Could not find dataset in VFB for paper ID {paper_id}")
            continue
            
        ds_id = ds_info[0]['ds_id']
        log_info(f"Dataset ID for paper {paper_id} is {ds_id}")
        
        # Check for each SKID if its neuron has a link to the dataset
        for skid in paper_skids:
            if skid not in skid_to_neuron_map:
                # SKID exists in CATMAID but not in VFB
                continue
                
            neuron = skid_to_neuron_map[skid]
            
            # Check if this neuron is linked to the dataset
            link_query = f"""
            MATCH (i:Individual {{short_form: '{neuron['vfb_id']}'}})
            MATCH (ds:DataSet {{short_form: '{ds_id}'}})
            RETURN exists((i)-[:has_source]->(ds)) as has_link
            """
            
            try:
                link_results = nc.commit_list([link_query])
                link_info = results_2_dict_list(link_results)
                
                if link_info and not link_info[0]['has_link']:
                    # Neuron exists but doesn't have a link to this dataset
                    cypher = f"""
                    // Link neuron {neuron['vfb_label']} to dataset {ds_id}
                    MATCH (n:Individual {{short_form: '{neuron['vfb_id']}'}})
                    MATCH (ds:DataSet {{short_form: '{ds_id}'}})
                    MERGE (n)-[:has_source {{iri: "http://purl.org/dc/terms/source", label: "has_source", short_form: "source", type: "Annotation"}}]->(ds)
                    """
                    cypher_queries.append(cypher)
                    total_missing += 1
            except Exception as e:
                log_error(f"Error checking link for neuron {neuron['vfb_id']} and dataset {ds_id}: {str(e)}")
                
    log_info(f"Total missing links found: {total_missing}")
    log_info(f"Generated {len(cypher_queries)} Cypher queries to fix missing links")
    
    # Save to file
    if report and cypher_queries:
        try:
            # Ensure the directory exists
            os.makedirs("../VFB_reporting_results/CATMAID_SKID_reports", exist_ok=True)
            
            outfile = f"../VFB_reporting_results/CATMAID_SKID_reports/{report}_missing_links.cypher"
            with open(outfile, 'w') as f:
                f.write("// Cypher queries to fix missing dataset links\n")
                f.write("// Generated on " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n\n")
                f.write("\n".join(cypher_queries))
            log_info(f"Saved {len(cypher_queries)} missing link queries to {outfile}")
        except Exception as e:
            log_error(f"Failed to save missing links report: {str(e)}")
    
    return cypher_queries


if __name__ == '__main__':
    # generate skid reports when this is run as a script

    # variables for generating reports
    # dict of sources and project IDs for larval datasets
    larval_sources = {'l1em': 1, 'abd1.5': 1, 'iav-robo': 1, 'iav-tnt': 4, 'l3vnc': 2}
    larval_reports = [["https://" + s + ".catmaid.virtualflybrain.org", larval_sources[s], "papers",
                       None, s.upper()] # Using None to trigger automatic annotation selection
                      for s in larval_sources.keys()]

    FAFB = ["https://fafb.catmaid.virtualflybrain.org", 1, "Published", None, "FAFB"]
    FANC1 = ["https://fanc.catmaid.virtualflybrain.org", 1, "publication", None, "FANC1"]
    FANC2 = ["https://fanc.catmaid.virtualflybrain.org", 2, "publication", None, "FANC2"]
    LEG40 = ["https://radagast.hms.harvard.edu/catmaidvnc", 61, "publication", None, "LEG40"]

    # Create output directory if it doesn't exist
    os.makedirs("../VFB_reporting_results/CATMAID_SKID_reports", exist_ok=True)

    # make reports with try/except blocks to prevent failures from stopping the process
    try:
        for r in larval_reports:
            gen_cat_skid_report_officialnames(*r)
            try:
                gen_missing_links_report(r[0], r[1], r[2], r[4])
            except Exception as e:
                log_error(f"Error generating missing links report for {r[4]}: {str(e)}")
    except Exception as e:
        log_error(f"Error processing larval reports: {str(e)}")
        
    try:
        gen_cat_skid_report_officialnames(*FAFB)
        try:
            gen_missing_links_report(*FAFB[0:3], FAFB[4])
        except Exception as e:
            log_error(f"Error generating missing links report for FAFB: {str(e)}")
    except Exception as e:
        log_error(f"Error processing FAFB report: {str(e)}")
    
    try:
        gen_cat_skid_report_officialnames(*FANC1)
        try:
            gen_missing_links_report(*FANC1[0:3], FANC1[4])
        except Exception as e:
            log_error(f"Error generating missing links report for FANC1: {str(e)}")
    except Exception as e:
        log_error(f"Error processing FANC1 report: {str(e)}")
    
    try:
        gen_cat_skid_report_officialnames(*FANC2)
        try:
            gen_missing_links_report(*FANC2[0:3], FANC2[4])
        except Exception as e:
            log_error(f"Error generating missing links report for FANC2: {str(e)}")
    except Exception as e:
        log_error(f"Error processing FANC2 report: {str(e)}")
    
    try:
        gen_cat_skid_report_officialnames(*LEG40)
        try:
            gen_missing_links_report(*LEG40[0:3], LEG40[4])
        except Exception as e:
            log_error(f"Error generating missing links report for LEG40: {str(e)}")
    except Exception as e:
        log_error(f"Error processing LEG40 report: {str(e)}")
