import pandas as pd
import datetime
import numpy

curator = 'cp390'  # change if needed

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

# get mapping of ds name (in VFB) to id (as index) from FAFB_comparison.tsv
comparison_table = pd.read_csv("../../VFB_reporting_results/FAFB_comparison.tsv", sep='\t',
                               index_col='Paper_ID').applymap(str)
comparison_table.index = comparison_table.index.map(str)

# get dataset name for paper from vfb
for i in paper_ids:
    DataSet = comparison_table['VFB_name'][i]  # dataset name in VFB
    if DataSet == str(numpy.nan):
        continue  # if no VFB dataset for paper

    single_ds_data = new_skids[new_skids['paper_id'] == i]
    curation_df = pd.DataFrame({'filename': single_ds_data['skid'],
                                'label': single_ds_data['name'], 'is_a': 'neuron',
                                'part_of': 'female organism|adult brain'})

    output_filename = './anat_%s_%s' % (DataSet, datestring)

    curation_df.to_csv(output_filename + '.tsv', sep='\t', index=None)

    with open(output_filename + '.yaml', 'w') as file:
        file.write("DataSet: %s\n" % DataSet)
        file.write("Curator: %s\n" % curator)

