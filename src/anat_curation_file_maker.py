import pandas as pd
import datetime
from vfb_connect.neo.neo4j_tools import Neo4jConnect, dict_cursor

nc = Neo4jConnect('http://kb.virtualflybrain.org', 'neo4j', 'neo4j')
curator = 'https://orcid.org/0000-0002-1373-1705'

"""
Makes image curation record(s) for new skids in VFB_reporting_results/FAFB_new_skids.tsv.
These should be transferred to curation repo for loading.

Ignores any skids whose catmaid paper ID is not associated with a DataSet in VFB.
This should not be automatically run every day (makes dated files for curation).
"""

# date for filenames
today = datetime.date.today()
datestring = today.strftime("%y%m%d")

# open file of new FAFB skids
new_skids = pd.read_csv("../../VFB_reporting_results/FAFB_new_skids.tsv", sep='\t')\
    .applymap(str)
paper_ids = set(list(new_skids['paper_id']))

# check whether all datasets in KB
comparison_table = pd.read_csv("../../VFB_reporting_results/FAFB_comparison.tsv", sep='\t')
if comparison_table['VFB_name'].isnull().values.any():
    print("One or more FAFB dataset(s) not in KB - these will be skipped.")

# get dataset names from vfb
for paper_id in paper_ids:
    query = "MATCH (ds:DataSet {catmaid_annotation_id:%s}) \
        RETURN ds.short_form as dataset" % paper_id

    q = nc.commit_list([query])
    try:
        DataSet = dict_cursor(q)[0]['dataset']
    except IndexError:
        continue

    single_ds_data = new_skids[new_skids['paper_id']==paper_id]
    curation_df = pd.DataFrame({'filename': single_ds_data['skid'],
                                'label': single_ds_data['name'], 'is_a': 'neuron',
                                'part_of': 'female organism|adult brain'})

    output_filename = './anat_%s_%s' % (DataSet, datestring)

    curation_df.to_csv(output_filename + '.tsv', sep='\t', index=None)

    with open(output_filename + '.yaml', 'w') as file:
        file.write("DataSet: %s\n" % DataSet)
        file.write("Curator: %s\n" % curator)

