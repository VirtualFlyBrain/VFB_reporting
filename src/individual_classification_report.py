from reporting_tools import gen_report
import pandas as pd
from collections import Counter

pd.options.display.float_format = "{:,.0f}".format

output_file = "../VFB_reporting_results/individual_classification_KB_PDB.tsv"
PDB_server = ('http://pdb.v4.virtualflybrain.org', 'neo4j', 'vfb')
KB_server = ('http://kb.p2.virtualflybrain.org', 'neo4j', 'vfb')


# get classifications of individuals from both servers and do inner join on individual short_form

KB_classification = gen_report(server=KB_server,
                               query=("MATCH (i:Individual)-[:INSTANCEOF]->(c:Class) "
                                      "WHERE c.short_form =~ 'FBbt_[0-9]+' "
                                      "RETURN i.short_form AS ind_ID, "
                                      "COLLECT(c.short_form) AS KB_FBbt_IDs"),
                               report_name='KB_classification')

PDB_classification = gen_report(server=PDB_server,
                                query=("MATCH (i:Individual)-[:INSTANCEOF]->(c:Class) "
                                       "WHERE c.short_form =~ 'FBbt_[0-9]+' "
                                       "RETURN i.short_form AS ind_ID, "
                                       "COLLECT(c.short_form) AS PDB_FBbt_IDs"),
                                report_name='PDB_classification')

KB_classification.set_index('ind_ID', inplace=True)
PDB_classification.set_index('ind_ID', inplace=True)

merged_classification = pd.merge(left=KB_classification, right=PDB_classification, how="inner", on="ind_ID")


# add columns for classification added to PDB and removed from KB


def list_diff(pair_of_lists):
    """Compares a pair of lists and returns (as 2 x pd.Series) items added and removed from the first list."""
    added = [i for i in pair_of_lists[1] if i not in pair_of_lists[0]]
    removed = [i for i in pair_of_lists[0] if i not in pair_of_lists[1]]
    return pd.Series([added, removed], index=['added', 'removed'])


diff = merged_classification.apply(list_diff, axis=1)

ind_classification_diff = pd.merge(left=merged_classification, right=diff, on='ind_ID')


# collapse added and removed columns into lists of FBbt IDs and count frequencies

added_list = list(ind_classification_diff['added'].explode().dropna())
added_freq = Counter(added_list)
added_df = pd.DataFrame.from_dict(data=added_freq, orient='index', columns=['times_added'])
added_df.index.name = "FBbt_ID"

removed_list = list(ind_classification_diff['removed'].explode().dropna())
removed_freq = Counter(removed_list)
removed_df = pd.DataFrame.from_dict(data=removed_freq, orient='index', columns=['times_removed'])
removed_df.index.name = "FBbt_ID"

fbbt_classification_diff = pd.merge(left=added_df, right=removed_df, how='outer', on="FBbt_ID")


# add FBbt labels, tidy up and export report to file

FBbt_labels = gen_report(server=KB_server,
                               query=("MATCH (c:Class) WHERE c.short_form IN %s "
                                      "RETURN c.short_form AS FBbt_ID, c.label AS FBbt_label"
                                      % (list(fbbt_classification_diff.index))),
                               report_name='FBbt_labels')

FBbt_labels.set_index("FBbt_ID", inplace=True)

fbbt_classification_diff = pd.merge(left=fbbt_classification_diff, right=FBbt_labels, how='left', on="FBbt_ID")
fbbt_classification_diff = fbbt_classification_diff[["FBbt_label", "times_added", "times_removed"]]
fbbt_classification_diff.fillna(0, inplace=True)
fbbt_classification_diff.sort_values(by="times_added", ascending=False, inplace=True)

fbbt_classification_diff.to_csv(output_file, sep='\t', float_format="{:.0f}".format)

