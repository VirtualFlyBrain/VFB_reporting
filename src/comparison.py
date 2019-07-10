import pandas as pd
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
nc = neo4j_connect('http://kb.virtualflybrain.org', 'neo4j', 'neo4j')

CAT_papers = pd.read_csv("CAT_datasets.tsv", sep="\t", index_col="id")
CAT_skids = pd.read_csv("CAT_skids.tsv", sep="\t")

# PAPERS
# get table of catmaid papers list from VFB

pub_query = "MATCH (ds:DataSet) WHERE ds.catmaid_annotation_id IS NOT NULL " \
            "RETURN ds.catmaid_annotation_id as id, ds.short_form as VFB_name"
q = nc.commit_list([pub_query])
papers = results_2_dict_list(q)

VFB_papers = pd.DataFrame.from_dict(papers)
VFB_papers = VFB_papers.set_index("id")

# compare papers in VFB and catmaid

all_papers = pd.concat([CAT_papers, VFB_papers], join="outer", axis=1, sort=True)
all_papers.to_csv("all_papers.tsv", sep="\t")

new_papers = all_papers[all_papers.VFB_name.isnull()]
print(str(len(new_papers.index)) + " new papers in CATMAID that are not in VFB:")
print(new_papers["name"])

# NEURON SKIDS
# get a list of all skids in VFB

skid_query = "MATCH ()-[r]->() WHERE r.catmaid_skeleton_ids IS NOT NULL RETURN DISTINCT r.catmaid_skeleton_ids"

q = nc.commit_list([skid_query])
skids = results_2_dict_list(q)  # is list of dicts of lists

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


