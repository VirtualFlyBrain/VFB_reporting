import pandas as pd
import json
import ast
from collections import defaultdict
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list

pd.set_option('display.max_columns', None)

nc = neo4j_connect('http://pdb.virtualflybrain.org', 'neo4j', 'vfb')

query = ("MATCH (i:Individual)-[r:database_cross_reference]->(s:Site:Connectome:Individual) "
         "MATCH (i)-[:INSTANCEOF]->(c:Class)-[x:has_reference]->(p:pub) "
         "WHERE c.short_form STARTS WITH 'FBbt' "
         "AND x.has_synonym_type IS NOT NULL "
         "RETURN s.short_form AS dataset, "
         "r.accession[0] AS source_id, "
         "i.short_form AS VFB_id, i.label AS label, "
         "COLLECT(DISTINCT c.label) AS parent_classes, "
         "COLLECT({has_synonym_type: x.has_synonym_type, "
         "value: x.value, publication: p.short_form}) AS parent_synonyms")

q = nc.commit_list([query])
results = results_2_dict_list(q)
results_df = pd.DataFrame(results)


# --- JSON-like synonym string parsing ---

def _parse_json_like(s):
    """Parse JSON or Python-literal strings into objects. Tries json.loads then ast.literal_eval."""
    if s is None or isinstance(s, dict):
        return s if isinstance(s, dict) else None
    if isinstance(s, (bytes, bytearray)):
        s = s.decode('utf-8')
    if not isinstance(s, str):
        return None

    s = s.strip()
    # Unwrap outer quotes if present
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # Try JSON
    try:
        return json.loads(s)
    except Exception:
        pass

    # Fallback to Python literal
    try:
        return ast.literal_eval(s)
    except Exception:
        return None


def _find_types_in_obj(obj):
    """Recursively find the 'has_synonym_type' field."""
    if obj is None or not isinstance(obj, (dict, list, tuple)):
        return None

    if isinstance(obj, dict):
        # Look for exact key 'has_synonym_type'
        if 'has_synonym_type' in obj:
            v = obj['has_synonym_type']
            if v is None:
                return None
            return list(v) if isinstance(v, (list, tuple)) else [v]

        # Recurse into nested structures
        for v in obj.values():
            if isinstance(v, (dict, list, tuple)):
                found = _find_types_in_obj(v)
                if found:
                    return found
        return None

    # Recurse into list/tuple items
    for item in obj:
        found = _find_types_in_obj(item)
        if found:
            return found
    return None


def _find_value_in_obj(obj):
    """Recursively find the 'value' field containing the synonym text."""
    if obj is None:
        return None

    if isinstance(obj, dict):
        # Look for exact 'value' key
        if 'value' in obj and obj['value'] is not None:
            return obj['value']

        # Recurse into nested structures
        for v in obj.values():
            if isinstance(v, (dict, list, tuple)):
                found = _find_value_in_obj(v)
                if found is not None:
                    return found
        return None

    if isinstance(obj, (list, tuple)):
        # Find first non-container item or recurse
        for item in obj:
            if not isinstance(item, (dict, list, tuple)):
                return item
            found = _find_value_in_obj(item)
            if found is not None:
                return found
        return None

    # Scalar value
    return obj


def parent_synonyms_list_to_dict(syn_list):
    """Convert a list of synonym dicts into a dict: has_synonym_type -> [values].

    Drops entries that have no detected type. Multiple types for a single value
    will result in that value appearing under each type.
    Strips 'http://purl.obolibrary.org/obo/fbbt#' prefix from type URIs.
    Keeps only synonym types containing 'name_in_'.
    """
    if syn_list is None:
        return {}

    out = defaultdict(list)
    for entry in syn_list:
        if entry is None:
            continue

        # Entry should be a dict with 'has_synonym_type' and 'value' keys
        if isinstance(entry, dict):
            type_uri = entry.get('has_synonym_type')
            value = entry.get('value')

            if type_uri is None or value is None:
                continue

            # Handle list of types
            types = type_uri if isinstance(type_uri, list) else [type_uri]

            # Add value under each type (strip URL prefix and filter for 'name_in_')
            for t in types:
                try:
                    type_str = str(t)
                    # Strip the fbbt URL prefix
                    if type_str.startswith('http://purl.obolibrary.org/obo/fbbt#'):
                        type_str = type_str.replace('http://purl.obolibrary.org/obo/fbbt#', '', 1)

                    # Keep only types containing 'name_in_'
                    if 'name_in_' in type_str:
                        out[type_str].append(str(value))
                except Exception:
                    continue

    # Deduplicate values per type while preserving order
    for key in list(out.keys()):
        seen = set()
        unique_values = []
        for v in out[key]:
            if v not in seen:
                seen.add(v)
                unique_values.append(v)
        if unique_values:
            out[key] = unique_values
        else:
            del out[key]

    return dict(out)


# Apply conversion to create a new column 'parent_synonym_dict'
results_df['parent_synonym_dict'] = results_df['parent_synonyms'].apply(
    lambda lst: parent_synonyms_list_to_dict(lst)
)

# Extract all unique synonym types across all rows
all_synonym_types = set()
for syn_dict in results_df['parent_synonym_dict']:
    all_synonym_types.update(syn_dict.keys())

# Create columns for each synonym type (flatten lists to comma-separated strings)
for syn_type in sorted(all_synonym_types):
    results_df[syn_type] = results_df['parent_synonym_dict'].apply(
        lambda d: ', '.join(d.get(syn_type, []))
    )

# Display results with base columns and synonym type columns
display_cols = ['dataset', 'source_id', 'VFB_id', 'label', 'parent_classes'] + sorted(list(all_synonym_types))
print(results_df[display_cols])

# Split output by dataset and save to TSV files
for dataset in results_df['dataset'].unique():
    dataset_df = results_df[results_df['dataset'] == dataset].copy()

    # Drop dataset column and parent_synonyms column
    output_cols = ['source_id', 'VFB_id', 'label', 'parent_classes'] + sorted(list(all_synonym_types))
    dataset_df = dataset_df[output_cols]

    # Save to TSV file without index
    output_file = f"../VFB_reporting_results/instance_synonym_report_{dataset}.tsv"
    dataset_df.to_csv(output_file, sep='\t', index=False)
    print(f"Saved {output_file}")



