from reporting_tools import gen_report

def get_scRNAseq_DataSets(vfb_server=('http://pdb.virtualflybrain.org', 'neo4j', 'vfb')):
    """Gets scRNAseq DataSets from VFB.
    Default server is pdb"""

    scRNAseq_DataSets = gen_report(server=vfb_server,
                             query=("MATCH (ds:scRNAseq_DataSet:Individual) "
                                    "RETURN DISTINCT ds.short_form AS FlyBase_ID, ds.label AS Name, "
                                    "ds.description[0] AS Description, ds.filtered_gene_count[0] AS `Filtered Gene Count` "
                                    "ORDER BY ds.label DESC"),
                             report_name='scRNAseq_DataSets')

    return scRNAseq_DataSets


if __name__ == "__main__":
    print("Getting scRNAseq DataSets")
    scRNAseq_DataSet_table = get_scRNAseq_DataSets()
    scRNAseq_DataSet_table.to_csv("../VFB_reporting_results/ID_tables/scRNAseq_DataSets.tsv", sep='\t', index=False)
