import pandas as pd
import datetime
from vfb_connect.neo.neo4j_tools import Neo4jConnect, dict_cursor

nc = Neo4jConnect('http://kb.virtualflybrain.org', 'neo4j', 'neo4j')
curator = 'https://orcid.org/0000-0002-1373-1705'  # change if needed

"""
Makes newmeta curation record(s) for skids in VFB_reporting_results/FAFB_CAT_cellType_skids.tsv.
These should be transferred to curation repo for loading.

Only adds new is_a annotations and ignores any skids that are not yet in VFB.
This should not be automatically run every day (makes dated files for curation).
"""

# date for filenames
today = datetime.date.today()
datestring = today.strftime("%y%m%d")

# open file of new FAFB skids
typed_skids = pd.read_csv("../../VFB_reporting_results/FAFB_CAT_cellType_skids.tsv", sep='\t')\
    .applymap(str)

# open file of all skids with paper detail
all_skids = pd.read_csv("../../VFB_reporting_results/EM_CATMAID_FAFB_skids.tsv", sep='\t')\
    .applymap(str)
paper_ids = set(list(all_skids['paper_id']))
print(paper_ids)

# merge info from all skids into typed skids
typed_skids_detail = pd.merge(left=typed_skids, right=all_skids, on='skid', how='left')
typed_skids_detail = typed_skids_detail.drop(['annotaion_id', 'annotation_id', 'paper_name'], axis=1)

# get FBbt labels from VFB
FBbt_list = [str(x).replace(':', '_') for x in set(typed_skids_detail['annotation_name'])]
query = "MATCH (c:Class) WHERE c.short_form IN %s \
    RETURN c.short_form AS FBbt_id, c.label AS FBbt_name" % FBbt_list

q = nc.commit_list([query])
labels = dict_cursor(q)
labels_df = pd.DataFrame(labels)
labels_df = labels_df.applymap(lambda x: x.replace('_', ':'))

# remove old labels from mapping table and merge in new (then reorder cols)
typed_skids_detail = pd.merge(left=typed_skids_detail, right=labels_df,
                              how='left', left_on='annotation_name', right_on='FBbt_id')
print(typed_skids_detail[:10])
typed_skids_detail.to_csv("typing.csv", index=None)
# get dataset names from vfb
"""
for paper_id in paper_ids:
    query = "MATCH (ds:DataSet {catmaid_annotation_id:%s}) \
        RETURN ds.short_form as dataset" % paper_id
"""
# THESE TYPE AND CATMAID NAME MAPPINGS ARE OUT OF DATE