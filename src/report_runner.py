from reporting_tools import diff_report, gen_dataset_report, gen_dataset_report_prod, save_report


kb_report = gen_dataset_report(["http://kb.virtualflybrain.org", "neo4j", "neo4j"], 'kb')

save_report(kb_report, "../VFB_reporting_results/kb_report.tsv")

pdb_report = gen_dataset_report_prod(["http://pdb.virtualflybrain.org", "neo4j", "neo4j"], 'pdb')
save_report(pdb_report, "../VFB_reporting_results/pdb_report.tsv")

try:
  staging_report = gen_dataset_report_prod(["http://pdb-alpha.virtualflybrain.org", "neo4j", "neo4j"], 'staging')
  save_report(staging_report, "../VFB_reporting_results/staging_report.tsv")
  staging_diff = diff_report(pdb_report, staging_report)
  save_report(staging_diff.staging_not_pdb, '../VFB_reporting_results/staging_diff.tsv')
except:
  print("An exception occurred running staging report!")

try:
  dev_report = gen_dataset_report_prod(["http://pdb-dev.virtualflybrain.org", "neo4j", "neo4j"], 'dev')
  save_report(dev_report, "../VFB_reporting_results/dev_report.tsv")
  dev_diff = diff_report(pdb_report, dev_report)
  save_report(dev_diff.dev_not_pdb, '../VFB_reporting_results/dev_diff.tsv')
except:
  print("An exception occurred running dev report!")
  




