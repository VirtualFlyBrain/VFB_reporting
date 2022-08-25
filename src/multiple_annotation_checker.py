from reporting_tools import gen_report
import re


def multi_annotation_report_gen(source):
    """Makes a tsv of EM images annotated with multiple FBbt terms."""
    output_file = "../VFB_reporting_results/multiple_annotations/%s_multiple_annotations.tsv" % source
    PDB_server = ('http://pdb.virtualflybrain.org', 'neo4j', 'vfb')
    KB_server = ('http://kb.virtualflybrain.org', 'neo4j', 'vfb')

    multi_annotated_inds_pdb = gen_report(server=PDB_server,
                                          query=("MATCH p=(n:Individual)-[r:INSTANCEOF]->(c:Class) "
                                                 "WHERE c.short_form CONTAINS 'FBbt' and "
                                                 "n.label CONTAINS '%s' WITH DISTINCT n, "
                                                 "COUNT(p) AS cp, COLLECT(c.short_form) as PDB_FBbt_ids, "
                                                 "COLLECT(c.label) AS PDB_FBbt_labels WHERE cp >1 "
                                                 "RETURN n.short_form AS Individual_id, n.label "
                                                 "AS Individual_label, PDB_FBbt_ids, PDB_FBbt_labels" % source),
                                          report_name='multi_annotated_inds_pdb')

    ind_ids = list(multi_annotated_inds_pdb['Individual_id'])

    annotations_in_kb = gen_report(server=KB_server,
                                   query=("MATCH (n:Individual)-[r:INSTANCEOF]->(c:Class) "
                                          "WHERE c.short_form CONTAINS 'FBbt' and "
                                          "n.short_form IN %s "
                                          "RETURN DISTINCT n.short_form AS Individual_id, "
                                          "COLLECT(c.short_form) AS KB_FBbt_ids, "
                                          "COLLECT(c.label) AS KB_FBbt_labels"
                                          % ('[\'' + '\', \''.join(ind_ids) + '\']')),
                                   report_name='annotations_in_kb')

    multi_annotated_inds = \
        multi_annotated_inds_pdb.set_index('Individual_id').join(annotations_in_kb.set_index('Individual_id'))

    # add column with skid
    pattern = '%s:([0-9]+)' % source
    comp_pattern = re.compile(pattern)
    multi_annotated_inds['External_id'] = multi_annotated_inds['Individual_label']. \
        map(lambda x: re.search(comp_pattern, x).group(1))
    multi_annotated_inds = multi_annotated_inds[[
        'Individual_label', 'External_id', 'PDB_FBbt_ids', 'PDB_FBbt_labels', 'KB_FBbt_ids', 'KB_FBbt_labels']]

    multi_annotated_inds.to_csv(output_file, sep='\t', index=None)


if __name__ == '__main__':
    for source in ['L1EM', 'FAFB', 'FlyEM-HB']:
        multi_annotation_report_gen(source)
