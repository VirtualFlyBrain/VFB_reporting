import pandas as pd
import datetime
import numpy
from vfb_connect.neo.neo4j_tools import Neo4jConnect, dict_cursor

nc = Neo4jConnect('http://kb.virtualflybrain.org', 'neo4j', 'neo4j')
curator = 'cp390'  # change if needed

"""
Makes newmeta curation record(s) for skids in VFB_reporting_results/FAFB_CAT_cellType_skids.tsv.
These should be transferred to curation repo for loading.
Run anat_curation_file_maker.py first to get any new skids into VFB.
Only adds new (not in VFB) is_a annotations.
This should not be automatically run every day (makes dated files for curation).
"""

# date for filenames
today = datetime.date.today()
datestring = today.strftime("%y%m%d")

# open file of new FAFB skids (no paper ids)
typed_skids = pd.read_csv("../../../VFB_reporting_results/FAFB_CAT_cellType_skids.tsv", sep='\t') \
    .applymap(str)

# open file of all skids, including paper ids
all_skids = pd.read_csv("../../../VFB_reporting_results/EM_CATMAID_FAFB_skids.tsv", sep='\t') \
    .applymap(str)

# get mapping of dataset name (in VFB) to id (as index) from FAFB_comparison.tsv
comparison_table = pd.read_csv("../../../VFB_reporting_results/FAFB_comparison.tsv", sep='\t',
                               index_col='Paper_ID').applymap(str)
comparison_table.index = comparison_table.index.map(str)

# merge info from all skids into typed skids, remove excess cols
typed_skids = pd.merge(left=typed_skids, right=all_skids, on='skid', how='left')
typed_skids = typed_skids.drop(['annotaion_id', 'annotation_id', 'paper_name'], axis=1)

# get FBbt labels from VFB
FBbt_list = [str(x).replace(':', '_') for x in set(typed_skids['annotation_name'])]
query = "MATCH (c:Class) WHERE c.short_form IN %s \
    RETURN c.short_form AS FBbt_id, c.label AS FBbt_name" % FBbt_list

q = nc.commit_list([query])
labels = dict_cursor(q)
labels_df = pd.DataFrame(labels)
labels_df = labels_df.applymap(lambda x: x.replace('_', ':'))

# merge FBbt labels into typed skids dataframe on FBbt ID
typed_skids = pd.merge(left=typed_skids, right=labels_df,
                       how='left', left_on='annotation_name', right_on='FBbt_id')
# typed_skids.to_csv("typing.csv", index=None)  # for checking mappings

# drop any annotations that are already in the KB (avoids proliferation of curation files)
# find all current skids and annotations (copied from comparison.py)
paper_ids = set(list(typed_skids['paper_id']))
query = "MATCH (api:API)<-[dsxref:hasDbXref]-(ds:DataSet)" \
        "<-[:has_source]-(i:Individual)" \
        "-[skid:hasDbXref]->(s:Site) " \
        "WHERE api.short_form in ['fafb_catmaid_api', 'l1em_catmaid_api'] " \
        "AND s.short_form in ['catmaid_fafb', 'catmaid_l1em'] " \
        "AND dsxref.accession in %s WITH i, skid " \
        "MATCH (i)-[:INSTANCEOF]-(c:Class) " \
        "RETURN distinct skid.accession AS `skid`, c.short_form AS `FBbt_id`" \
        % ('[' + ', '.join(paper_ids) + ']')

q = nc.commit_list([query])
skids_in_paper_vfb = dict_cursor(q)
vfb_skid_classes_df = pd.DataFrame.from_dict(skids_in_paper_vfb)
vfb_skid_classes_df = vfb_skid_classes_df.applymap(lambda x: str(x).replace('_', ':'))
vfb_skid_classes_df['VFB'] = 'VFB'  # for identifying matches

# merge existing mappings into typed skids df and delete any rows that matched
all_skid_mappings = pd.merge(
    left=typed_skids, right=vfb_skid_classes_df, on=['FBbt_id', 'skid'], how='left')
new_skid_mappings = all_skid_mappings[all_skid_mappings['VFB'].isnull()]  # rows that not in VFB

# get dataset names from vfb
paper_ids = set(list(new_skid_mappings['paper_id']))
for i in paper_ids:
    ds = comparison_table['VFB_name'][i]  # dataset name in VFB
    if ds == str(numpy.nan):
        continue  # if no VFB dataset for paper

    single_ds_data = new_skid_mappings[new_skid_mappings['paper_id'] == i]
    curation_df = pd.DataFrame({'subject_external_db': 'catmaid_fafb',
                                'subject_external_id': single_ds_data['skid'],
                                'relation': 'is_a',
                                'object': single_ds_data['FBbt_name']})

    output_filename = './newmeta_%s_%s' % (ds, datestring)

    curation_df.to_csv(output_filename + '.tsv', sep='\t', index=None)

    with open(output_filename + '.yaml', 'w') as file:
        file.write("DataSet: %s\n" % ds)
        file.write("Curator: %s\n" % curator)
