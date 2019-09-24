
# get_catmaid_papers
## gen_cat_paper_report(URL, PROJECT_ID, paper_annotaion, report_name):
    Gets IDs and names of papers in CATMAID and returns a dataframe.
    
## gen_cat_skid_report(URL, PROJECT_ID, paper_annotaion, report_name):
    Gets IDs and names of papers in CATMAID, then skids for the neurons in each paper and returns a dataframe.
    NB. each skid may feature in multiple papers.

## gen_cat_skid_report_officialnames(URL, PROJECT_ID, paper_annotaion, name_annotation, report_name):
    Gets IDs and names of papers in CATMAID, then skids for the neurons in each paper and returns a dataframe.
    This version collects the official names from annotations that are annotaed with a name_annotation 
    NB. each skid may feature in multiple papers.
