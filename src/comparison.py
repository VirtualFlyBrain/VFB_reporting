import pandas as pd
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
import get_catmaid_papers

nc = neo4j_connect('http://kb.virtualflybrain.org', 'neo4j', 'neo4j')

# variables for generating reports

L1EM = [get_catmaid_papers.gen_cat_paper_report(
    "https://l1em.catmaid.virtualflybrain.org", 1, "papers", "L1_CAT"),
    get_catmaid_papers.gen_cat_skid_report(
    "https://l1em.catmaid.virtualflybrain.org", 1, "papers", "L1_CAT"),
    "L1EM"]

FAFB = [get_catmaid_papers.gen_cat_paper_report(
    "https://fafb.catmaid.virtualflybrain.org", 1, "Published", "FAFB_CAT"),
    get_catmaid_papers.gen_cat_skid_report_officialnames_fbbt(
    "https://fafb.catmaid.virtualflybrain.org", 1, "Published", "neuron name", "11078097", "FAFB_CAT"),
    "FAFB"]


def make_catmaid_vfb_reports(cat_papers, cat_skids, dataset_name):
    """Make comparison with data in VFB for given sets of papers and skids in CATMAID.

    Outputs a file of numbers of SKIDs per paper and a file of CATMAID SKIDs that are not in VFB."""

    comparison_outfile = "../VFB_reporting_results/" + dataset_name + "_comparison.tsv"
    # comparison_outfile = dataset_name + "_comparison.tsv"  # for local use
    skids_outfile = "../VFB_reporting_results/" + dataset_name + "_new_skids.tsv"
    # skids_outfile = dataset_name + "_new_skids.tsv"  # for local use

    # Get table of names of catmaid datasets in VFB
    pub_query = "MATCH (ds:DataSet) WHERE ds.catmaid_annotation_id IS NOT NULL " \
                "RETURN ds.catmaid_annotation_id as CATMAID_ID, ds.short_form as VFB_name"
    q = nc.commit_list([pub_query])
    papers = results_2_dict_list(q)

    vfb_papers = pd.DataFrame.from_dict(papers)
    vfb_papers = vfb_papers.set_index("CATMAID_ID")

    # match up SKIDs per paper and output dict of lists of skids
    skids_by_paper = {}
    for paper_id in cat_papers.index:  # do everything per paper

        # get list of skids in CATMAID data as strings
        skids_in_paper_cat = [str(s) for s in cat_skids[cat_skids['paper_id'] == paper_id]['skid']]

        # get skids from VFB KB and reformat to list of strings
        query = "MATCH (ds:DataSet {catmaid_annotation_id : " + str(paper_id) + \
                "})<-[:has_source]-()<-[]-()-[r:in_register_with]->() WHERE r.catmaid_skeleton_ids" \
                " IS NOT NULL RETURN DISTINCT r.catmaid_skeleton_ids"
        q = nc.commit_list([query])
        skids_in_paper_vfb = results_2_dict_list(q)

        skids_in_paper_vfb = [s['r.catmaid_skeleton_ids'] for s in skids_in_paper_vfb]  # flatten to list
        skids_in_paper_vfb = [s.replace("[", "") for s in skids_in_paper_vfb]  # remove brackets
        skids_in_paper_vfb = [s.replace("]", "") for s in skids_in_paper_vfb]  # remove brackets

        # comparison of lists of skids
        cat_not_vfb = [s for s in skids_in_paper_cat if s not in skids_in_paper_vfb]
        vfb_not_cat = [s for s in skids_in_paper_vfb if s not in skids_in_paper_cat]
        skids_by_paper[paper_id] = {'skids_in_paper_cat': skids_in_paper_cat, 'skids_in_paper_vfb': skids_in_paper_vfb,
                                    'cat_not_vfb': cat_not_vfb, 'vfb_not_cat': vfb_not_cat}

    # make dataframe of list lengths from skid_df
    skids_df = pd.DataFrame.from_dict(skids_by_paper, orient='index')  # df of lists
    skids_df_count = skids_df.applymap(lambda x: len(x))

    # make combined table with all info, tidy up and save as tsv
    all_papers = pd.merge(cat_papers, vfb_papers, left_index=True, right_index=True, how='left', sort=True)
    all_papers = pd.concat([all_papers, skids_df_count], join="outer", axis=1, sort=True)
    all_papers.rename(columns={'name': 'CATMAID_name', 'VFB_name': 'VFB_name',
                               'skids_in_paper_cat': 'CATMAID_SKIDs', 'skids_in_paper_vfb': 'VFB_SKIDS',
                               'cat_not_vfb': 'CATMAID_not_VFB', 'vfb_not_cat': 'VFB_not_CATMAID'},
                      inplace=True)
    all_papers.index.name = 'Paper_ID'
    all_papers.to_csv(comparison_outfile, sep="\t")

    # make unique set of skids in vfb (sum function concatenates empty lists where no skids for a paper)
    vfb_skid_list = \
        list(set(sum([[int(skid) for skid in skids_df['skids_in_paper_vfb'][i]] for i in skids_df.index], [])))

    # filter cat_skids dataframe (df_skids from get_catmaid_papers) to remove rows where skid in VFB
    new_skids_output = cat_skids[~cat_skids['skid'].isin(vfb_skid_list)].sort_values('skid') \
        .reindex(columns=(set(cat_skids.columns.tolist() + ['FBbt_ID'])))
    new_skids_output.to_csv(skids_outfile, sep="\t", index=False)  # output file

    # TERMINAL OUTPUT
    new_papers = all_papers[all_papers.VFB_name.isnull()]
    print(str(len(new_papers.index)) + " new papers in CATMAID that are not in VFB")
    print(new_papers["CATMAID_name"])
    print("See " + comparison_outfile + " for differences in numbers of SKIDs")
    print("See " + skids_outfile + " for new SKIDs that are not yet in VFB")


make_catmaid_vfb_reports(*L1EM)
make_catmaid_vfb_reports(*FAFB)
