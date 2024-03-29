from reporting_tools import diff_report, gen_dataset_report, gen_dataset_report_prod, save_report


kb_report = gen_dataset_report(["http://kb.virtualflybrain.org", "neo4j", "vfb"], 'kb')
save_report(kb_report, "../VFB_reporting_results/kb_report.tsv")

pdb_report = gen_dataset_report_prod(["http://pdb.virtualflybrain.org", "neo4j", "vfb"], 'pdb')
save_report(pdb_report, "../VFB_reporting_results/pdb_report.tsv")


try:
  pipeline_output_report = gen_dataset_report_prod(["http://pdb.ug.virtualflybrain.org", "neo4j", "vfb"], 'pipeline_output')
  save_report(pipeline_output_report, "../VFB_reporting_results/pipeline_output_report.tsv")
  pipeline_output_diff = diff_report(pdb_report, pipeline_output_report)
  save_report(pipeline_output_diff, '../VFB_reporting_results/pdb_pipeline_output_diff.tsv')
except:
  print("An exception occurred running pipeline output report!")

try:
  staging_report = gen_dataset_report_prod(["http://pdb-alpha.virtualflybrain.org", "neo4j", "vfb"], 'staging')
  save_report(staging_report, "../VFB_reporting_results/staging_report.tsv")
  staging_diff = diff_report(pdb_report, staging_report)
  save_report(staging_diff, '../VFB_reporting_results/pdb_staging_diff.tsv')
except:
  print("An exception occurred running staging report!")

try:
  dev_report = gen_dataset_report_prod(["http://pdb-dev.virtualflybrain.org", "neo4j", "vfb"], 'dev')
  save_report(dev_report, "../VFB_reporting_results/dev_report.tsv")
  dev_diff = diff_report(pdb_report, dev_report)
  save_report(dev_diff, '../VFB_reporting_results/pdb_dev_diff.tsv')
except:
  print("An exception occurred running dev report!")
  




