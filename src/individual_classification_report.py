from reporting_tools import gen_report
import pandas as pd
from collections import Counter
import logging
import time
import sys

# Set up logging
log_file = "../VFB_reporting_results/individual_classification_report.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, mode='w'),  # Use write mode to clear the log file on each run
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

pd.options.display.float_format = "{:,.0f}".format

output_file = "../VFB_reporting_results/individual_classification_KB_PDB.tsv"
PDB_server = ('http://pdb.virtualflybrain.org', 'neo4j', 'vfb')
KB_server = ('http://kb.virtualflybrain.org', 'neo4j', 'vfb')

logger.info("Starting individual classification report generation")
start_time = time.time()

# Function to handle batch processing of FBbt IDs
def get_fbbt_labels_in_batches(server, fbbt_ids, batch_size=50):
    """Gets FBbt labels in batches to avoid HTTP 415 errors"""
    logger.info(f"Processing {len(fbbt_ids)} FBbt IDs in batches of {batch_size}")
    all_labels = pd.DataFrame(columns=["FBbt_ID", "FBbt_label"])
    
    # Process in batches
    for i in range(0, len(fbbt_ids), batch_size):
        batch = fbbt_ids[i:i+batch_size]
        logger.debug(f"Processing batch {i//batch_size + 1}/{(len(fbbt_ids) + batch_size - 1)//batch_size}")
        
        try:
            query = ("MATCH (c:Class) WHERE c.short_form IN %s "
                    "RETURN c.short_form AS FBbt_ID, c.label AS FBbt_label" 
                    % str(batch))
            
            batch_labels = gen_report(
                server=server,
                query=query,
                report_name=f'FBbt_labels_batch_{i//batch_size + 1}'
            )
            
            if not batch_labels.empty:
                all_labels = pd.concat([all_labels, batch_labels], ignore_index=True)
        except Exception as e:
            logger.error(f"Error processing batch {i}-{i+batch_size}: {str(e)}")
    
    logger.info(f"Retrieved labels for {len(all_labels)} FBbt IDs")
    return all_labels

# Try to get classifications of individuals from both servers
try:
    logger.info("Fetching KB classification data...")
    KB_classification = gen_report(server=KB_server,
                                query=("MATCH (i:Individual)-[:INSTANCEOF]->(c:Class) "
                                        "WHERE c.short_form =~ 'FBbt_[0-9]+' "
                                        "RETURN i.short_form AS ind_ID, "
                                        "COLLECT(c.short_form) AS KB_FBbt_IDs"),
                                report_name='KB_classification')
    logger.info(f"Retrieved {len(KB_classification)} KB classifications")

    logger.info("Fetching PDB classification data...")
    PDB_classification = gen_report(server=PDB_server,
                                    query=("MATCH (i:Individual)-[:INSTANCEOF]->(c:Class) "
                                        "WHERE c.short_form =~ 'FBbt_[0-9]+' "
                                        "RETURN i.short_form AS ind_ID, "
                                        "COLLECT(c.short_form) AS PDB_FBbt_IDs"),
                                    report_name='PDB_classification')
    logger.info(f"Retrieved {len(PDB_classification)} PDB classifications")
except Exception as e:
    logger.error(f"Error retrieving classification data: {str(e)}")
    sys.exit(1)

# Process the data
try:
    logger.info("Processing classification data...")
    KB_classification.set_index('ind_ID', inplace=True)
    PDB_classification.set_index('ind_ID', inplace=True)

    merged_classification = pd.merge(left=KB_classification, right=PDB_classification, how="inner", on="ind_ID")
    logger.info(f"Merged classifications: {len(merged_classification)} matching individual IDs")

    def list_diff(pair_of_lists):
        """Compares a pair of lists and returns (as 2 x pd.Series) items added and removed from the first list."""
        added = [i for i in pair_of_lists[1] if i not in pair_of_lists[0]]
        removed = [i for i in pair_of_lists[0] if i not in pair_of_lists[1]]
        return pd.Series([added, removed], index=['added', 'removed'])

    diff = merged_classification.apply(list_diff, axis=1)
    ind_classification_diff = pd.merge(left=merged_classification, right=diff, on='ind_ID')

    # Process added items
    added_list = list(ind_classification_diff['added'].explode().dropna())
    added_freq = Counter(added_list)
    added_df = pd.DataFrame.from_dict(data=added_freq, orient='index', columns=['times_added'])
    added_df.index.name = "FBbt_ID"
    logger.info(f"Found {len(added_df)} FBbt terms added in PDB")

    # Process removed items
    removed_list = list(ind_classification_diff['removed'].explode().dropna())
    removed_freq = Counter(removed_list)
    removed_df = pd.DataFrame.from_dict(data=removed_freq, orient='index', columns=['times_removed'])
    removed_df.index.name = "FBbt_ID"
    logger.info(f"Found {len(removed_df)} FBbt terms removed from KB")

    fbbt_classification_diff = pd.merge(left=added_df, right=removed_df, how='outer', on="FBbt_ID")
    logger.info(f"Total distinct FBbt terms changed: {len(fbbt_classification_diff)}")
except Exception as e:
    logger.error(f"Error processing classification data: {str(e)}")
    sys.exit(1)

# Get FBbt labels using batch processing to avoid 415 errors
try:
    logger.info("Retrieving FBbt labels...")
    fbbt_ids = list(fbbt_classification_diff.index)
    
    # Use batch processing instead of a single large query
    FBbt_labels = get_fbbt_labels_in_batches(KB_server, fbbt_ids)
    
    if not FBbt_labels.empty:
        FBbt_labels.set_index("FBbt_ID", inplace=True)
        
        fbbt_classification_diff = pd.merge(
            left=fbbt_classification_diff, 
            right=FBbt_labels, 
            how='left', 
            on="FBbt_ID"
        )
        
        # Check for missing labels
        missing_labels = fbbt_classification_diff[fbbt_classification_diff['FBbt_label'].isna()]
        if not missing_labels.empty:
            logger.warning(f"Missing labels for {len(missing_labels)} FBbt terms")
        
        fbbt_classification_diff = fbbt_classification_diff[["FBbt_label", "times_added", "times_removed"]]
        fbbt_classification_diff.fillna(0, inplace=True)
        fbbt_classification_diff.sort_values(by="times_added", ascending=False, inplace=True)
    else:
        logger.error("Failed to retrieve any FBbt labels")
except Exception as e:
    logger.error(f"Error retrieving FBbt labels: {str(e)}")
    # Continue execution to output what we have

# Save the report
try:
    logger.info(f"Saving report to {output_file}")
    fbbt_classification_diff.to_csv(output_file, sep='\t', float_format="{:.0f}".format)
    logger.info(f"Report saved successfully with {len(fbbt_classification_diff)} rows")
except Exception as e:
    logger.error(f"Error saving report: {str(e)}")
    sys.exit(1)

# Report completion
elapsed_time = time.time() - start_time
logger.info(f"Process completed in {elapsed_time:.2f} seconds")

