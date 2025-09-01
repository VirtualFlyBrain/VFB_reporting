"""Checks a set of labels to whether any that are on EM individuals
are not on the classes that they are annotated with."""

import pandas as pd
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
from oaklib import get_adapter
import wget
import pathlib

nc = neo4j_connect('http://pdb.virtualflybrain.org', 'neo4j', 'vfb')
adapter = get_adapter("sqlite:obo:fbbt")

# nt labels
neurotransmitter_labels = ['Cholinergic', 'Glutamatergic', 'GABAergic', 'Octopaminergic', 'Dopaminergic',
                           'Serotonergic', 'Histaminergic', 'Tyraminergic']

# lineage labels
lineage_file_url = 'https://raw.githubusercontent.com/FlyBase/drosophila-anatomy-developmental-ontology/master/src/patterns/data/all-axioms/neuroblastAnnotations.tsv'
wget.download(lineage_file_url)
lineage_df = pd.read_csv('neuroblastAnnotations.tsv', dtype='str', sep='\t')
pathlib.Path("neuroblastAnnotations.tsv").unlink(missing_ok=True)

## drop sex-specific
lineage_df = lineage_df[~lineage_df['defined_class_label'].str.contains('male|female')]

## create lineage label list
lineage_labels = ['primary_neuron', 'secondary_neuron']
for nomenclature_col in ['ito_lee', 'hartenstein', 'primary', 'secondary']:
    label_list = ['lineage_' + lineage_df[nomenclature_col][i]
                  for i in lineage_df[lineage_df[nomenclature_col].notna()].index]
    lineage_labels = lineage_labels + label_list

# exclude some high-level classes
excluded_ids = ['FBbt:00052046', 'FBbt:00047097', 'FBbt:00058205', 'FBbt:00047096', 'FBbt:00052517',
                'FBbt:00052518', 'FBbt:00059289', 'FBbt:00058207', 'FBbt:00058208', 'FBbt:00049540',
                'FBbt:00049539', 'FBbt:00001987', 'FBbt:00058244', 'FBbt:00001985', 'FBbt:00059276',
                'FBbt:00048286', 'FBbt:00007577', 'FBbt:00001984', 'FBbt:00001986', 'FBbt:00005922',
                'FBbt:00047105', 'FBbt:00048301', 'FBbt:00048933', 'FBbt:00067123', 'FBbt:00051420',
                'FBbt:00048944', 'FBbt:00047106', 'FBbt:00047247', 'FBbt:00051439', 'FBbt:00048931',
                'FBbt:00048943', 'FBbt:00058202', 'FBbt:00058206', 'FBbt:00001496', 'FBbt:00052007',
                'FBbt:00003870', 'FBbt:00059273', 'FBbt:00048235', 'FBbt:00052132', 'FBbt:00051206',
                'FBbt:00051207', 'FBbt:00048138', 'FBbt:00007440', 'FBbt:00047517', 'FBbt:00007516',
                'FBbt:00058249', 'FBbt:00111640', 'FBbt:00059244', 'FBbt:00049517', 'FBbt:00049526',
                'FBbt:00049570', 'FBbt:00051301', 'FBbt:00051966', 'FBbt:00051303', 'FBbt:00058203',
                'FBbt:00003859', 'FBbt:00003882']

query = ("MATCH (i:Individual:Neuron:has_neuron_connectivity)-[:INSTANCEOF]->(c:Class) "
         "WHERE c.short_form STARTS WITH 'FBbt_' "
         "RETURN i.short_form AS instance_id, i.label AS instance_label, labels(i) AS instance_tags, "
         "c.short_form AS FBbt_id, c.label AS FBbt_label, labels(c) AS class_tags")
output = nc.commit_list([query])
all_neo_labels = pd.DataFrame(results_2_dict_list(output))
all_neo_labels['FBbt_id'] = all_neo_labels['FBbt_id'].apply(lambda x: x.replace('_', ':'))
all_neo_labels = all_neo_labels[~all_neo_labels['FBbt_id'].isin(excluded_ids)]

def label_conflict_report(query_labels, VFB_labels, report_filename):

    def label_filter(input_list, keep_labels, drop_labels):
        filtered_list = [x for x in input_list if (x in keep_labels) and not (x in drop_labels)]
        return filtered_list

    all_relevant_labels = pd.DataFrame(
        {'instance_id': VFB_labels['instance_id'], 'instance_label': VFB_labels['instance_label'],
         'instance_tags': VFB_labels['instance_tags'].apply(lambda x: label_filter(x, query_labels, [])),
         'FBbt_id': VFB_labels['FBbt_id'], 'FBbt_label': VFB_labels['FBbt_label'],
         'class_tags': VFB_labels['class_tags'].apply(lambda x: label_filter(x, query_labels, []))})

    all_relevant_labels['new_labels'] = all_relevant_labels.apply(lambda x:
                                        label_filter(x['instance_tags'], x['instance_tags'], x['class_tags']), axis=1)

    all_labels_with_new = all_relevant_labels[all_relevant_labels['new_labels'].str.len() > 0].reset_index(drop=True)
    all_labels_with_new['new_labels'] = all_labels_with_new['new_labels'].apply(lambda x: x[0])

    old_labels = all_labels_with_new[['FBbt_id', 'class_tags']]
    old_labels['existing_labels'] = old_labels['class_tags'].apply(lambda x: '|'.join(x))
    old_labels = old_labels.drop('class_tags', axis=1).drop_duplicates()

    value_counts = pd.DataFrame(all_labels_with_new[['FBbt_id', 'FBbt_label', 'new_labels']].value_counts().reset_index())
    value_counts = value_counts.merge(old_labels, on='FBbt_id', how='left').rename(
        columns={'class_tags': 'existing_labels'}).drop_duplicates()
    value_counts['subclasses'] = value_counts['FBbt_id'].apply(
        lambda x: adapter.descendant_count(x, predicates=['rdfs:subClassOf']))
    value_counts = value_counts.sort_values(by='FBbt_id', axis=0)
    value_counts.to_csv(report_filename, sep='\t', index=False)

if __name__ == '__main__':
    label_conflict_report(neurotransmitter_labels, all_neo_labels,
                          '../VFB_reporting_results/instance_FBbt_neurotransmitter_conflicts.tsv')
    label_conflict_report(lineage_labels, all_neo_labels,
                          '../VFB_reporting_results/instance_FBbt_lineage_conflicts.tsv')
