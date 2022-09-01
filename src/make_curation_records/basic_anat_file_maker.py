import pandas as pd
import datetime
import numpy
import re


def generate_comments(annotation_series):
    """
    Updates each set of annotations in a Series and returns a series
    """
    comments = annotation_series.apply(lambda x: ', '.join(
        [re.sub(' \([0-9]+\)', '', ann) for ann in x.split(', ') if 'Paper:' not in ann]))
    return comments


def make_anat_records(site, curator, output_filename='anat'):
    """
    Makes image curation record(s) for new skids.
    These should be transferred to curation repo for loading.

    Ignores any skids whose catmaid paper ID is not associated with a DataSet in VFB.
    This should not be automatically run every day (makes dated files for curation).
    """
    if site not in ['L1EM', 'FAFB', 'FANC1', 'FANC2', 'VNC1']:
        raise KeyError('site must be L1EM, FAFB, FANC1 OR FANC2')
    # date for filenames
    today = datetime.date.today()
    datestring = today.strftime("%y%m%d")

    save_directory = "src/make_curation_records/"
    read_directory = "../VFB_reporting_results/CATMAID_SKID_reports/"

    # open file of new skids
    new_skids = pd.read_csv("%s%s_new_skids.tsv" % (read_directory, site), sep='\t') \
        .applymap(str)
    paper_ids = set(list(new_skids['paper_id']))

    # get mapping of ds name (in VFB) to id (as index) from <site>_comparison.tsv
    comparison_table = pd.read_csv("%s%s_comparison.tsv" % (read_directory, site),
                                   sep='\t', index_col='Paper_ID').applymap(str)
    comparison_table.index = comparison_table.index.map(str)

    # get dataset name for paper from vfb
    for i in paper_ids:
        ds = comparison_table['VFB_name'][i]  # dataset name in VFB
        if ds == str(numpy.nan):
            continue  # if no VFB dataset for paper

        single_ds_data = new_skids[new_skids['paper_id'] == i]
        if site == 'L1EM':
            entity = 'embryonic/larval nervous system'
            template = 'L1 larval CNS ssTEM - Cardona/Janelia_c'
            instance = 'L1EM'
        elif site == 'FAFB':
            entity = 'female organism|adult brain'
            template = 'JRC2018Unisex_c'
            instance = 'FAFB'
        elif site in ['FANC1', 'FANC2']:
            entity = 'female organism|adult ventral nerve cord'
            template = 'JRC2018UnisexVNC_c'
            instance = 'FANC'
        curation_df = pd.DataFrame({'filename': single_ds_data['skid'],
                                    'label': single_ds_data['name'],
                                    'is_a': 'neuron',
                                    'part_of': entity,
                                    'comment': generate_comments(single_ds_data['annotations'])})
        curation_df['dbxrefs'] = curation_df['filename'].map(
            lambda x: str('catmaid_%s:%s' % (site.lower(), x)))
        if 'synonyms' in single_ds_data.keys():
            curation_df['synonyms'] = single_ds_data['synonyms']
        if single_ds_data['skid'].to_string() not in curation_df['label'].to_string():
            curation_df['label'] = curation_df['label'] + ' (' + instance + ':' + single_ds_data['skid'] + ')'
        #         curation_df['label'] = curation_df[['label', 'filename']].apply(lambda x: ' '.join(x), axis=1)

        if output_filename == "anat":
            output_filename1 = 'anat_%s_%s' % (ds, datestring)
        else:
            output_filename1 = output_filename + '_' + ds
        curation_df.to_csv(save_directory + output_filename1 + '.tsv', sep='\t', index=None)

        with open(save_directory + output_filename1 + '.yaml', 'w') as file:
            file.write("DataSet: %s\n" % ds)
            file.write("Curator: %s\n" % curator)
            file.write("Imaging_type: TEM\n")
            file.write("Template: %s\n" % template)


if __name__ == "__main__":
    make_anat_records('FAFB', 'cp390')
    #make_anat_records('L1EM', 'travis', '../VFB_reporting_results/anat_l1em_missing')
    #make_anat_records('FANC1', 'travis', '../VFB_reporting_results/anat_fanc1_missing')
