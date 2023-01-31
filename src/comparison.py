import pandas as pd
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
import get_catmaid_papers

nc = neo4j_connect('http://kb.virtualflybrain.org', 'neo4j', 'vfb')

# variables for generating reports

# dict of sources and project IDs for larval datasets
larval_sources = {'l1em': 1, 'abd1.5': 1, 'iav-robo': 1, 'iav-tnt': 2, 'l3vnc': 2}
larval_reports = [[get_catmaid_papers.gen_cat_paper_report(
    "https://" + s + ".catmaid.virtualflybrain.org", larval_sources[s], "papers"),
    get_catmaid_papers.gen_cat_skid_report_officialnames(
    "https://" + s + ".catmaid.virtualflybrain.org", larval_sources[s], "papers",
    ["neuron name", "MB nomenclature"]), s.upper()] for s in larval_sources.keys()]

FAFB = [get_catmaid_papers.gen_cat_paper_report(
    "https://fafb.catmaid.virtualflybrain.org", 1, "Published"),
    get_catmaid_papers.gen_cat_skid_report_officialnames(
    "https://fafb.catmaid.virtualflybrain.org", 1, "Published", ["neuron name"]),
    "FAFB"]

FANC1 = [get_catmaid_papers.gen_cat_paper_report(
   "https://fanc.catmaid.virtualflybrain.org", 1, "publication"),
   get_catmaid_papers.gen_cat_skid_report_officialnames(
   "https://fanc.catmaid.virtualflybrain.org", 1, "publication", ["neuron name"]),
   "FANC1"]

FANC2 = [get_catmaid_papers.gen_cat_paper_report(
   "https://fanc.catmaid.virtualflybrain.org", 2, "publication"),
   get_catmaid_papers.gen_cat_skid_report_officialnames(
   "https://fanc.catmaid.virtualflybrain.org", 2, "publication", ["neuron name"]),
   "FANC2"]

LEG40 = [get_catmaid_papers.gen_cat_paper_report(
   "https://radagast.hms.harvard.edu/catmaidvnc", 61, "publication"),
   get_catmaid_papers.gen_cat_skid_report_officialnames(
   "https://radagast.hms.harvard.edu/catmaidvnc", 61, "publication", ["neuron name"]),
   "LEG40"]


def make_catmaid_vfb_reports(cat_papers, cat_skids, dataset_name):
    """Make comparison with data in VFB for given sets of papers and skids in CATMAID.

    Outputs a file of numbers of SKIDs per paper and a file of CATMAID SKIDs that are not in VFB."""
    save_directory = "../VFB_reporting_results/CATMAID_SKID_reports/"
    comparison_outfile = save_directory + dataset_name + "_comparison.tsv"
    skids_outfile = save_directory + dataset_name + "_new_skids.tsv"
    neuron_skids_outfile = save_directory + dataset_name + "_neuron_only_skids.tsv"

    # Get table of names of catmaid datasets in VFB
    pub_query = "MATCH (api:API)<-[dsxref:database_cross_reference]-(ds:DataSet) " \
                "WHERE api.short_form ends with '_catmaid_api' " \
                "RETURN toInteger(dsxref.accession[0]) as CATMAID_ID, ds.short_form as VFB_name"
    try:
        q = nc.commit_list([pub_query])
        papers = results_2_dict_list(q)
    
        vfb_papers = pd.DataFrame.from_dict(papers)
        vfb_papers = vfb_papers.set_index("CATMAID_ID")
    except:
        vfb_papers = pd.DataFrame.from_dict({})
    # match up SKIDs per paper and output dict of lists of skids
    skids_by_paper = {}
    for paper_id in cat_papers.index:  # do everything per paper

        # get list of skids in CATMAID data as strings
        skids_in_paper_cat = [str(s) for s in cat_skids[cat_skids['paper_id'] == paper_id]['skid']]

        # get skids from VFB KB and reformat to list of strings
        query = "MATCH (api:API)<-[dsxref:database_cross_reference]-(ds:DataSet)" \
                "<-[:has_source]-(i:Individual)" \
                "-[skid:database_cross_reference]->(s:Site) " \
                "WHERE api.short_form ends with '_catmaid_api' " \
                "AND s.short_form starts with 'catmaid_' " \
                "AND dsxref.accession = ['" + str(paper_id) +"'] WITH i, skid " \
                "MATCH (i)-[:INSTANCEOF]-(c:Class) " \
                "RETURN distinct skid.accession[0] AS `r.catmaid_skeleton_ids`, c.iri"

        try:
            q = nc.commit_list([query])
            skids_in_paper_vfb = results_2_dict_list(q)
            vfb_skid_classes_df = pd.DataFrame.from_dict(skids_in_paper_vfb)

            # count skids only annotated as 'neuron'
        
            unique_skids = list(set([x for x in vfb_skid_classes_df['r.catmaid_skeleton_ids']]))
        except KeyError:  # if dataframe is empty make empty list of skids and empty df with named columns
            unique_skids = []
            vfb_skid_classes_df = pd.DataFrame(columns=['c.iri', 'r.catmaid_skeleton_ids'])

        neuron_iris = ['http://purl.obolibrary.org/obo/FBbt_00005106', 'http://purl.obolibrary.org/obo/CL_0000540']
        neuron_only_skids = []
        for skid in unique_skids:
            if len(vfb_skid_classes_df[vfb_skid_classes_df['r.catmaid_skeleton_ids'] == skid].index) == \
                    len(vfb_skid_classes_df[(vfb_skid_classes_df['r.catmaid_skeleton_ids'] == skid)
                                            & (vfb_skid_classes_df['c.iri'].isin(neuron_iris))].index):
                neuron_only_skids.append(skid)

        skids_in_paper_vfb = unique_skids # take unique records only

        # comparison of lists of skids
        cat_not_vfb = [s for s in skids_in_paper_cat if s not in skids_in_paper_vfb]
        vfb_not_cat = [s for s in skids_in_paper_vfb if s not in skids_in_paper_cat]
        skids_by_paper[paper_id] = {'skids_in_paper_cat': skids_in_paper_cat, 'skids_in_paper_vfb': skids_in_paper_vfb,
                                    'cat_not_vfb': cat_not_vfb, 'vfb_not_cat': vfb_not_cat,
                                    'neuron_only': neuron_only_skids}

    # make dataframe of list lengths from skid_df
    skids_df = pd.DataFrame.from_dict(skids_by_paper, orient='index')  # df of lists
    skids_df_count = skids_df.applymap(lambda x: len(x))

    # make combined table with all info, tidy up and save as tsv
    all_papers = pd.merge(cat_papers, vfb_papers, left_index=True, right_index=True, how='left', sort=True)
    all_papers = pd.concat([all_papers, skids_df_count], join="outer", axis=1, sort=True)
    all_papers.rename(columns={'name': 'CATMAID_name', 'VFB_name': 'VFB_name',
                               'skids_in_paper_cat': 'CATMAID_SKIDs', 'skids_in_paper_vfb': 'VFB_SKIDS',
                               'cat_not_vfb': 'CATMAID_not_VFB', 'vfb_not_cat': 'VFB_not_CATMAID',
                               'neuron_only': 'neuron_only'},
                      inplace=True)
    all_papers.index.name = 'Paper_ID'
    all_papers.to_csv(comparison_outfile, sep="\t")

    # make unique set of skids in vfb
    vfb_skid_list = [skid for skidlist in skids_df['skids_in_paper_vfb'] for skid in skidlist
                     if skid is not None]
    vfb_skid_list = list(set(vfb_skid_list))
    vfb_skid_list = [int(x) for x in vfb_skid_list]

    # filter cat_skids dataframe (df_skids from get_catmaid_papers) to remove rows where skid in VFB
    new_skids_output = cat_skids[~cat_skids['skid'].isin(vfb_skid_list)].sort_values('skid') \
        .reindex(columns=(cat_skids.columns.tolist() + ['FBbt_ID']))
    new_skids_output.to_csv(skids_outfile, sep="\t", index=False)  # output file

    # make unique set of skids annotated only as neuron in VFB
    vfb_neuron_skid_list = [skid for skidlist in skids_df['neuron_only'] for skid in skidlist
                     if skid is not None]
    vfb_neuron_skid_list = list(set(vfb_neuron_skid_list))
    vfb_neuron_skid_list = [int(x) for x in vfb_neuron_skid_list]

    # output file with skids only annotated as neuron
    neuron_skids_output = cat_skids[cat_skids['skid'].isin(vfb_neuron_skid_list)].sort_values('paper_id') \
        .reindex(columns=(cat_skids.columns.tolist() + ['FBbt_ID']))
    neuron_skids_output.to_csv(neuron_skids_outfile, sep="\t", index=False)

    # TERMINAL OUTPUT
    try:
        new_papers = all_papers[all_papers.VFB_name.isnull()]
        print(str(len(new_papers.index)) + " new papers in CATMAID that are not in VFB")
        print(new_papers["CATMAID_name"])
    except:
        print("No papers!!")
    print("See " + comparison_outfile + " for differences in numbers of SKIDs")
    print("See " + skids_outfile + " for new SKIDs that are not yet in VFB")


for r in larval_reports:
    make_catmaid_vfb_reports(*r)
make_catmaid_vfb_reports(*FAFB)
make_catmaid_vfb_reports(*FANC1)
make_catmaid_vfb_reports(*FANC2)
make_catmaid_vfb_reports(*LEG40)
