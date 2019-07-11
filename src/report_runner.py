from reporting_tools import diff_report, gen_dataset_report, gen_dataset_report_prod, save_report


kb_report = gen_dataset_report(["http://kb.virtualflybrain.org", "neo4j", "neo4j"], 'kb')
print(kb_report.name)

save_report(kb_report, "results/kb_report.tsv")

pdb_report = gen_dataset_report_prod(["http://pdb.virtualflybrain.org", "neo4j", "neo4j"], 'pdb')

staging_report = gen_dataset_report_prod(["http://pdb-alpha.virtualflybrain.org", "neo4j", "neo4j"], 'staging')

dev_report = gen_dataset_report_prod(["http://pdb-dev.virtualflybrain.org", "neo4j", "neo4j"], 'dev')

staging_diff = diff_report(pdb_report, staging_report)
dev_diff = diff_report(pdb_report, dev_report)
print(type(staging_diff))
print(type(dev_diff))

save_report(staging_diff.staging_not_pdb, 'results/staging_diff.tsv')
save_report(dev_diff.dev_not_pdb, 'results/dev_diff.tsv')


