import pandas as pd
import datetime
from vfb_connect import vfb

vfb.setNeoEndpoint('http://kb.virtualflybrain.org', usr='neo4j', pwd='vfb')
curator = 'cp390'  # change if needed

"""
Makes newmeta curation record(s) for skids in VFB_reporting_results/FAFB_all_skids_officialnames.tsv 
where typings are present in Management/FAFB/FAFB_skid_FBbt.tsv.
Records should be transferred to curation repo for loading.
Run anat_curation_file_maker.py first to get any new skids into VFB.
Only adds new (not in VFB) is_a annotations.
This should not be automatically run every day (makes dated files for curation).
"""

# date for filenames
today = datetime.date.today()
datestring = today.strftime("%y%m%d")

# open file of new FAFB skids (no paper ids) - from Management not Reporting results
typed_skids = pd.read_csv("../../../Management/FAFB/FAFB_skid_FBbt.tsv",
                          sep='\t', dtype='str')
typed_skids = typed_skids[typed_skids['FBbt_id'].notna()]

# open file of all skids, including paper ids
all_skids = pd.read_csv("../../../VFB_reporting_results/CATMAID_SKID_reports/FAFB_all_skids_officialnames.tsv",
                        sep='\t', dtype='str')

# get mapping of dataset name (in VFB) to id (as index) from FAFB_comparison.tsv
comparison_table = pd.read_csv("../../../VFB_reporting_results/CATMAID_SKID_reports/FAFB_comparison.tsv",
                               sep='\t', index_col='Paper_ID', dtype='str')
comparison_table.index = comparison_table.index.map(str)

# merge info from all skids into typed skids (by skid), remove excess columns
typed_skids = pd.merge(left=typed_skids, right=all_skids, on='skid', how='left')
typed_skids = typed_skids.drop(['paper_name', 'synonyms'], axis=1)

# get FBbt labels from VFB
FBbt_list = typed_skids['FBbt_id'].drop_duplicates().apply(lambda x: x.replace(':', '_')).to_list()
query1 = ("MATCH (c:Class) WHERE c.short_form IN %s "
          "RETURN c.short_form AS FBbt_id, c.label AS FBbt_name" % FBbt_list)
labels_df = vfb.cypher_query(query1)
labels_df['FBbt_id'] = labels_df['FBbt_id'].apply(lambda x: x.replace('_', ':'))

# merge FBbt labels into typed skids dataframe on FBbt ID
typed_skids = typed_skids.merge(right=labels_df, how='left', on='FBbt_id')

# typed_skids.to_csv("typing.csv", index=None)  # for checking mappings

# drop any annotations that are already in the KB (avoids proliferation of curation files)
# find all current skids and annotations (copied from comparison.py)
paper_ids = typed_skids.loc[typed_skids['paper_id'].notna(), 'paper_id'].drop_duplicates().to_list()
query2 = ("MATCH (api:API)<-[dsxref:database_cross_reference]-(ds:DataSet)"
          "<-[:has_source]-(i:Individual)"
          "-[skid:database_cross_reference]->(s:Site) "
          "WHERE api.short_form ends with '_catmaid_api' "
          "AND s.short_form starts with 'catmaid_' "
          "AND dsxref.accession[0] in %s WITH i, skid "
          "MATCH (i)-[:INSTANCEOF]-(c:Class) "
          "RETURN distinct skid.accession[0] AS `skid`, c.short_form AS `FBbt_id`"
          % paper_ids)
vfb_skid_classes_df = vfb.cypher_query(query2)
vfb_skid_classes_df['FBbt_id'] = vfb_skid_classes_df['FBbt_id'].apply(lambda x: x.replace('_', ':'))
vfb_skid_classes_df['VFB'] = 'VFB'  # for identifying matches

# merge existing mappings into typed skids df and delete any rows that matched
all_skid_mappings = typed_skids.merge(
    right=vfb_skid_classes_df, on=['FBbt_id', 'skid'], how='left')
new_skid_mappings = all_skid_mappings[all_skid_mappings['VFB'].isnull()]  # keep only rows that not in VFB
new_skid_mappings = new_skid_mappings[new_skid_mappings['paper_id'].notna()]  # drop skids not linked to a paper

# get reference (FBrf or doi) for dataset publications (as dataframe)
dataset_names = list(set(list(comparison_table['VFB_name'])))  # all ds names
query3 = ("MATCH (ds:DataSet)-[has_reference]->(p:pub) WHERE ds.short_form IN %s "
          "RETURN ds.short_form, p.DOI[0], p.FlyBase[0]"
          % dataset_names)
dataset_ref_df = vfb.cypher_query(query3)

# make a curation record for each dataset
paper_ids = set(list(new_skid_mappings['paper_id']))
for i in paper_ids:
    # get VFB dataset names
    try:
        ds = comparison_table['VFB_name'][i]  # dataset name in VFB
    except KeyError:
        continue  # if no row for paper in comparison table
    if comparison_table['VFB_name'].isnull()[i]:
        continue  # if no VFB dataset for paper

    single_ds_data = new_skid_mappings[new_skid_mappings['paper_id'] == i]

    # build dataframe of info for curation record
    curation_df = pd.DataFrame({'subject_external_db': 'catmaid_fafb',
                                'subject_external_id': single_ds_data['skid'],
                                'relation': 'is_a',
                                'object': single_ds_data['FBbt_name']})

    output_filename = './newmeta_%s_%s' % (ds, datestring)

    # drop any rows without a mapping and save file if it has rows
    curation_df = curation_df[curation_df['object'].notnull()]
    if len(curation_df.index) > 0:
        curation_df.to_csv(output_filename + '.tsv', sep='\t', index=None)

        with open(output_filename + '.yaml', 'w') as file:
            file.write("DataSet: %s\n" % ds)
            file.write("Curator: %s\n" % curator)

    else:
        continue
