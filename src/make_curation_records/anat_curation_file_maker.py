import pandas as pd
import datetime
import numpy
import vfb_connect
from vfb_connect.cross_server_tools import VfbConnect
import pysolr
passed = {'hair plate':'mechanosensory neuron of hair plate','campaniform sensillum':'sensory neuron of campaniform sensillum','T3 leg club chordotonal neuron':'metathoracic femoral chordotonal club neuron','T2 leg claw chordotonal neuron':'mesothoracic femoral chordotonal claw neuron','right T1 ventral nerve':'adult ventral prothoracic nerve','left T1 ventral nerve':'adult ventral prothoracic nerve','T1 leg claw chordotonal neuron':'prothoracic femoral chordotonal claw neuron','T1 leg club chordotonal neuron':'prothoracic femoral chordotonal club neuron','T1 leg hook chordotonal neuron':'prothoracic femoral chordotonal hook neuron'}
missing = {}
used = []
ref_terms = ['UPDATED', 'LINKED', 'Paper', 'et al.', ' from ', '?', 'on server','assigned to','update ','need to ']

def find_offical_label(term):
    solr = pysolr.Solr('https://solr.p2.virtualflybrain.org/solr/ontology/')
    for ref in ref_terms:
        if ref in term:
            return ''
    if term in missing.keys():
        return ''
    if term in passed.keys():
        return passed[term]
    expanded = []
    expanded.append(term)
    replacements = {'T1 ':'prothoracic ', 'T3 ':'metathoracic ', 'T2 ':'mesothoracic '}
    modified = term
    for rep in replacements:
        modified=modified.replace(rep,replacements[rep])
    expanded.append(modified)
    if not ' soma' in term and ('left ' in term or 'right ' in term):
        expanded.append(term.replace('left ','').replace('right ', ''))
        expanded.append('adult ' + term.replace('left ','').replace('right ', ''))
        expanded.append(modified.replace('left ','').replace('right ', ''))
        expanded.append('adult ' + modified.replace('left ','').replace('right ', ''))
    expanded.append('adult ' + term) #TODO check stage
    expanded.append('adult ' + modified)
    if 'DUM' in term:
        expanded.append(term.replace('DUM','dorsal unpaired median'))
        expanded.append(modified.replace('DUM','dorsal unpaired median'))
    if not ' ' in term:
        expanded.append(term + ' neuron')
        expanded.append(modified + ' neuron')
        expanded.append('adult ' + term + ' neuron')
        expanded.append('adult ' + modified + ' neuron')
    expanded = list(set(expanded))
    expanded.sort(key=len, reverse=True)
    for test in expanded:
        results = solr.search('label:"' + test + '" OR synonym:"' + test + '"')
        for doc in results.docs:
            if test in doc['synonym'] and 'FBbt' in doc['short_form']:
                passed.update({term:doc['label']})
                return doc['label']
    if not term in missing.keys() and not ':' in term:
        missing[term] = expanded
    return ''

def resolve_entity(entity="",annotation_series=[]):
    if not 'adult ventral nerve cord' in entity:
        return entity
    entityRows = []
    if 'adult ventral nerve cord' in entity:
        for annotations in annotation_series:
            if 'motor neuron' in annotations or 'sensory neuron' in annotations or 'nerve' in annotations:
                entityRows.append(entity.replace('ventral nerve cord','nervous system'))
            else:
                entityRows.append(entity)
        return pd.Series(entityRows)
    return entity

def find_available_terms(annotation_series=[]):
    """
    Return a '|' delimited list of annotation labels that exists as an FBbt term in pdb.ug
    """
    vc = VfbConnect(neo_endpoint='http://pdb.ug.virtualflybrain.org')
    results = []
    for annotations in annotation_series:
        names = []
        result = "neuron"
        for annotation in annotations.split('), '):
            if not annotation.split(' (')[0] in names and not annotation.split(' (')[0] == "neuron" and not 'nerve' in annotation.split(' (')[0]:
                name = find_offical_label(annotation.split(' (')[0])
                if len(name) > 0 and not name in names:
                    names.append(name)
        for name in names:
            try:
                id = vc.lookup_id(name)
                if 'FBbt' in id:
                    result += '|' + name
                else:
                    if not name in missing.keys():
                        missing[name] = annotations
            except:
                if not name in missing.keys():
                    missing[name] = annotations
        results.append(result);
    return pd.Series(results)

def find_dbxrefs(annotation_series=[]):
    """
    Return a '|' delimited Series of dbxrefs form annotaions
    """
    xrefs = []
    for annotations in annotation_series:
        result = ""
        for annotation in annotations.split('), '):
            name = annotation.split(' (')[0]
            if 'project id 2 on server https://catmaid3.hms.harvard.edu/catmaidvnc' in name:
                if 'LINKED NEURON - elastic transformation and flipped of skeleton id ' in name:
                    result += "|" + name.replace('LINKED NEURON - elastic transformation and flipped of skeleton id ','catmaid_fanc:').replace(' in project id 2 on server https://catmaid3.hms.harvard.edu/catmaidvnc','')
                else:
                    result += "|" + name.replace('LINKED NEURON - elastic transformation of skeleton id ','catmaid_fanc:').replace(' in project id 2 on server https://catmaid3.hms.harvard.edu/catmaidvnc','')
            if 'source: https://gen1mcfo.janelia.org/cgi-bin/view_gen1mcfo_imagery.cgi?line=' in name:
                result += "|" + name.replace('source: https://gen1mcfo.janelia.org/cgi-bin/view_gen1mcfo_imagery.cgi?line=','FlyLightGen1MCFO:')
            if 'project id 59 on server https://catmaid3.hms.harvard.edu/catmaidvnc' in name:
                if 'LINKED NEURON - pruned (first entry, last exit) by vol 109 of skeleton id ' in name:
                    result += "|" + name.replace('LINKED NEURON - pruned (first entry, last exit) by vol 109 of skeleton id ','catmaid_fanc_JRC2018VF:').replace(' in project id 59 on server https://catmaid3.hms.harvard.edu/catmaidvnc','')
                if 'LINKED NEURON - radius pruned of skeleton id ' in name:
                    result += "|" + name.replace('LINKED NEURON - radius pruned of skeleton id ','catmaid_fanc_JRC2018VF:').replace(' in project id 59 on server https://catmaid3.hms.harvard.edu/catmaidvnc','')
        xrefs.append(result)
    return pd.Series(xrefs)

def generate_comments(annotation_series=[]):
    """
    Return a ', ' delimited Series of comments form annotaions
    """
    comments = []
    for annotations in annotation_series:
        result = ""
        for annotation in annotations.split('), '):
            name = annotation.split(' (')[0]
            for ref in ref_terms:
                if ref in name:
                    name = ''
            if len(result) > 0 and len(name) > 0:
                name = ', ' + name
            if ': ' in name and not 'Paper:' in name:
                result += name
            else:
                for use in [' from ',' soma','pruned',' flipped', 'nerve']:
                    if use in name:
                        result += name
                        break
                if not name.replace(', ','') in result:
                    result += name
        comments.append(result)
    return pd.Series(comments)

def create_metadata(db="",filename_series=[],annotation_series=[],pub=""):
    results=[]
    soma_rules={'left soma':'left side of organism','right soma':'right side of organism','midline soma':'cell body rind along midline of adult ventral nerve cord'}
    nerve_rules={}
    for id,annotations in zip(filename_series,annotation_series):
        if ' soma' in annotations:
            for key in soma_rules.keys():
                if key in annotations:
                    results.append({'object':soma_rules[key],'relation':'has soma location','subject_external_id':id,'subject_external_db':db,'pub':pub})
        else:
            if 'left ' in annotations:
                results.append({'object':'left side of organism','relation':'overlaps','subject_external_id':id,'subject_external_db':db,'pub':pub})
            if 'right ' in annotations:
                results.append({'object':'right side of organism','relation':'overlaps','subject_external_id':id,'subject_external_db':db,'pub':pub})
        if 'nerve' in annotations:
            for annotation in annotations.split('), '):
                name = find_offical_label(annotation.split(' (')[0])
                if len(name) > 0 and ('nerve' in name or 'nerve' in annotation):
                    results.append({'object':name,'relation':'fasciculates_with','subject_external_id':id,'subject_external_db':db,'pub':pub})
        if 'motor neuron' in annotations or 'sensory neuron' in annotations or 'nerve' in annotations:
            results.append({'object':'peripheral nervous system','relation':'overlaps','subject_external_id':id,'subject_external_db':db,'pub':pub})
            results.append({'object':'adult ventral nerve cord','relation':'overlaps','subject_external_id':id,'subject_external_db':db,'pub':pub})
    return pd.DataFrame(results)

def make_anat_records(site, curator, output_filename='./anat'):
    """
    Makes image curation record(s) for new skids in VFB_reporting_results/<site>_new_skids.tsv.
    These should be transferred to curation repo for loading.

    Ignores any skids whose catmaid paper ID is not associated with a DataSet in VFB.
    This should not be automatically run every day (makes dated files for curation).
    """
    if site not in ['L1EM', 'FAFB', 'FANC1', 'FANC2', 'VNC1']:
        raise KeyError('site must be L1EM, FAFB, FANC1 OR FANC2')
    # date for filenames
    today = datetime.date.today()
    datestring = today.strftime("%y%m%d")

    save_directory = "../VFB_reporting_results/CATMAID_SKID_reports/"

    # open file of new skids
    new_skids = pd.read_csv("%s%s_new_skids.tsv" % (save_directory, site), sep='\t')\
        .applymap(str)
    paper_ids = set(list(new_skids['paper_id']))

    # get mapping of ds name (in VFB) to id (as index) from <site>_comparison.tsv
    comparison_table = pd.read_csv("%s%s_comparison.tsv" % (save_directory, site),
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
                                    'label': single_ds_data['name'].map(
                                        lambda x: str('%s' % (x.replace(' - elastic transform','')))),
                                    'is_a': find_available_terms(single_ds_data['annotations']),
                                    'part_of': resolve_entity(entity,single_ds_data['annotations']),
                                    'comment': generate_comments(single_ds_data['annotations'])})
        curation_df['dbxrefs'] = curation_df['filename'].map(
            lambda x: str('catmaid_%s:%s' % (site.lower().replace('fanc2','fanc_JRC2018VF'), x)))
        curation_df['dbxrefs'] += find_dbxrefs(single_ds_data['annotations'])
        if 'synonyms' in single_ds_data.keys():
            curation_df['synonyms'] = single_ds_data['synonyms']
        if single_ds_data['skid'].to_string() not in curation_df['label'].to_string():
            curation_df['label'] = curation_df['label'] + \
                ' (' + instance + ':' + single_ds_data['skid'] + ')'
#         curation_df['label'] = curation_df[['label', 'filename']].apply(lambda x: ' '.join(x), axis=1)

        if output_filename == "./anat":
            output_filename1 = './anat_%s_%s' % (ds, datestring)
        else:
            output_filename1 = output_filename + '_' + ds
        curation_df.to_csv(output_filename1 + '.tsv', sep='\t', index=None)
        output_filename2 = output_filename + '_' + ds
        create_metadata(db=site.lower().replace('fanc2','fanc_JRC2018VF'),filename_series=single_ds_data['skid'],annotation_series=single_ds_data['annotations']).to_csv(output_filename1.replace('anat_','meta_') + '.tsv', sep='\t', index=None)

        with open(output_filename1 + '.yaml', 'w') as file:
            file.write("DataSet: %s\n" % ds)
            file.write("Curator: %s\n" % curator)
            file.write("Imaging_type: TEM\n")
            file.write("Template: %s\n" % template)


if __name__ == "__main__":
    make_anat_records('FAFB', 'travis',
                      '../VFB_reporting_results/anat_fafb_missing')
    missing.clear() #not used YET on ^
    make_anat_records('L1EM', 'travis',
                      '../VFB_reporting_results/anat_l1em_missing')
    missing.clear() #not used YET on ^
    make_anat_records('FANC1', 'travis',
                      '../VFB_reporting_results/anat_fanc1_missing')
    pd.DataFrame({'missing terms': pd.Series(missing.keys()),'meta':pd.Series(missing.values())}).to_csv('../VFB_reporting_results/CATMAID_SKID_reports/anat_fanc1_missing_terms.tsv', sep='\t', index=None)
    pd.DataFrame({'CATMAID annotation': pd.Series(passed.keys()),'FBbt term':pd.Series(passed.values())}).to_csv('../VFB_reporting_results/CATMAID_SKID_reports/anat_fanc1_resolved_terms.tsv', sep='\t', index=None)
    missing.clear()
    make_anat_records('FANC2', 'travis', '../VFB_reporting_results/anat_fanc2_missing')
    pd.DataFrame({'missing terms': pd.Series(missing.keys()),'meta':pd.Series(missing.values())}).to_csv('../VFB_reporting_results/CATMAID_SKID_reports/anat_fanc2_missing_terms.tsv', sep='\t', index=None)
    pd.DataFrame({'CATMAID annotation': pd.Series(passed.keys()),'FBbt term':pd.Series(passed.values())}).to_csv('../VFB_reporting_results/CATMAID_SKID_reports/anat_fanc2_resolved_terms.tsv', sep='\t', index=None)

