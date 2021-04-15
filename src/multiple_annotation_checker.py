from reporting_tools import gen_report
import re

def multi_annotation_report_gen(source):
    """Makes a tsv of EM images annotated with multiple FBbt terms."""
    output_file = "../VFB_reporting_results/multiple_annotations/%s_multiple_annotations.tsv" % source
    VFB_server = ('http://pdb.virtualflybrain.org', 'neo4j', 'neo4j')

    multi_annotated_inds = gen_report(server=VFB_server,
                                      query=("MATCH p=(n:Individual)-[r:INSTANCEOF]->(c:Class) "
                                             "WHERE c.short_form CONTAINS 'FBbt' and "
                                             "n.label CONTAINS '%s' WITH distinct n, "
                                             "count(p) as cp, collect(c.short_form) as FBbt_ids, "
                                             "collect(c.label) as FBbt_labels WHERE cp >1 "
                                             "RETURN n.short_form as Individual_id, n.label "
                                             "as Individual_label, FBbt_ids, FBbt_labels" % source),
                                      report_name='multi_annotated_inds')

    # add column with skid
    pattern = '%s:([0-9]+)' % source
    comp_pattern = re.compile(pattern)
    multi_annotated_inds['Skid'] = multi_annotated_inds['Individual_label'].\
        map(lambda x: re.search(comp_pattern, x).group(1))
    multi_annotated_inds = multi_annotated_inds[[
        'Individual_id', 'Individual_label', 'External_id', 'FBbt_ids', 'FBbt_labels']]

    multi_annotated_inds.to_csv(output_file, sep='\t', index=None)


if __name__ == '__main__':
    for source in ['L1EM', 'FAFB', 'FlyEM-HB']:
        multi_annotation_report_gen(source)

