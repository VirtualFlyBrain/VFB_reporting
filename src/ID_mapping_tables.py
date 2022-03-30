from reporting_tools import gen_report

site_list = ['catmaid_fafb', 'catmaid_fanc', 'catmaid_l1em', 'neuronbridge', \
             'neuprint_JRC_Hemibrain_1point1', 'FlyCircuit']


def get_ids(site_name, vfb_server=('http://pdb.v4.virtualflybrain.org', 'neo4j', 'vfb')):
    """Gets neuron IDs from VFB for a given :Site (e.g. catmaid_fafb).
    site_name should be the short_form of the :Site
    Default server is pdb.v4"""

    neuron_data = gen_report(server=vfb_server,
                             query=("MATCH (n:Neuron:Individual)-[d:database_cross_reference]->(s) "
                                    "WHERE s.short_form=\"%s\" OPTIONAL MATCH (n)-[:INSTANCEOF]->(f:Class:Anatomy) "
                                    "WHERE f.short_form STARTS WITH \"FBbt\""
                                    "RETURN DISTINCT n.short_form AS VFB_ID, d.accession AS external_IDs, "
                                    "COLLECT(f.label) AS cell_types" % site),
                             report_name='neuron_data')

    return neuron_data


if __name__ == "__main__":
    for site in site_list:
        print("Getting IDs for %s" % site)
        id_table = get_ids(site)
        id_table.to_csv("../VFB_reporting_results/ID_tables/%s_ID_table.tsv" % site, sep='\t', index=False)
