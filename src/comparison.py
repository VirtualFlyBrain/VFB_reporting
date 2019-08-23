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
    get_catmaid_papers.gen_cat_skid_report(
    "https://fafb.catmaid.virtualflybrain.org", 1, "Published", "FAFB_CAT"),
    "FAFB"]


def make_catmaid_vfb_reports(cat_papers, cat_skids, dataset_name):
    """Return a comparison with data in VFB for given sets of papers and skids in CATMAID"""

    # Get table of names of catmaid datasets in VFB
    outfile = dataset_name + "_comparison.tsv"

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

    # make dataframe of list lengths from skid_diff
    skids_df = pd.DataFrame.from_dict(skids_by_paper, orient='index')
    skids_df = skids_df.applymap(lambda x: len(x))

    # make combined table with all info, tidy up and save as tsv
    all_papers = pd.merge(cat_papers, vfb_papers, left_index=True, right_index=True, how='left', sort=True)
    all_papers = pd.concat([all_papers, skids_df], join="outer", axis=1, sort=True)
    all_papers.rename(columns={'name': 'CATMAID_name', 'VFB_name': 'VFB_name',
                               'skids_in_paper_cat': 'CATMAID_SKIDs', 'skids_in_paper_vfb': 'VFB_SKIDS',
                               'cat_not_vfb': 'CATMAID_not_VFB', 'vfb_not_cat': 'VFB_not_CATMAID'},
                      inplace=True)
    all_papers.index.name = 'Paper_ID'
    all_papers.to_csv(outfile, sep="\t")

    # TODO - make table of skids requiring mapping

    # TERMINAL OUTPUT

    # new papers
    new_papers = all_papers[all_papers.VFB_name.isnull()]
    print(str(len(new_papers.index)) + " new papers in CATMAID that are not in VFB")
    print(new_papers["CATMAID_name"])
    print("See " + outfile + " for differences in numbers of SKIDs")


make_catmaid_vfb_reports(*L1EM)
make_catmaid_vfb_reports(*FAFB)


"""
# SKID details - OBSOLETE?
# get a list of all skids in VFB
skid_query = "MATCH ()-[r]->() WHERE r.catmaid_skeleton_ids IS NOT NULL RETURN DISTINCT r.catmaid_skeleton_ids"
q = nc.commit_list([skid_query])
skids = results_2_dict_list(q)

skid_list = [s['r.catmaid_skeleton_ids'] for s in skids]  # flatten to a list
skid_list = [s.replace("[", "") for s in skid_list]  # remove brackets(!)
skid_list = [s.replace("]", "") for s in skid_list]  # remove brackets(!)

# compare neuron lists

new_skids = CAT_skids.loc[~CAT_skids['skid'].isin(skid_list), ]
new_skids.to_csv("new_skids.tsv", sep="\t", index=False)
print(str(len(new_skids.index)) + " new skids in CATMAID that are not in VFB (see \'new_skids.tsv\')")

# old skids in new papers

old_skids_new_papers = CAT_skids.loc[CAT_skids['skid'].isin(skid_list)
                                     & CAT_skids['paper_id'].isin(new_papers.index), ]
old_skids_new_papers.to_csv("old_skids_new_papers.tsv", sep="\t", index=False)
print(str(len(old_skids_new_papers.index)) + " skids that are already in VFB, but have been used by new papers in"
                                             " CATMAID that are not in VFB (see \'old_skids_new_papers.tsv\')")
"""
