"""Reports connectome neurons (Individual:Neuron:has_neuron_connectivity) that are
INSTANCEOF more than one Class carrying a symbol.

Background: the Circuit Browser labels each graph node with the symbol of the class the
neuron is an instance of. When a neuron is an instance of several symbol-bearing classes
the displayed label is ambiguous. Where one of those classes is a subclass of all the
others the label resolves to that leaf (resolvable_by_subclass = True). Where there is no
such class (resolvable_by_subclass = False) the competing classes are not linked in the
ontology hierarchy - either a subclass relationship is missing, or the neuron carries
conflicting cell-type annotations. Those are the rows to review; the instance `comment`
usually holds the raw source typing that explains them.

A report is generated for each pipeline server (pdb = production, dev/staging =
pre-release, checked during the release process). Each offender is cross-checked against
the knowledge base (kb), which is the curation source upstream of the pipeline:

  conflict_in_kb = True  -> kb itself is INSTANCEOF >=2 of the competing classes, so the
                            ambiguous typing originates in the curation source (fix in kb).
  conflict_in_kb = False -> kb carries <2 of them, so the extra symbol-bearing class is
                            introduced downstream by the pipeline (investigate the pipeline).

The kb_typing column shows the neuron's full INSTANCEOF classification in kb for
comparison. (kb has neither the has_neuron_connectivity label nor class symbols - those
are added by the pipeline - so the kb check compares the INSTANCEOF class set by
short_form rather than re-running the symbol query.)
"""

import json
import pandas as pd
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list

REPORT_SERVERS = {
    'pdb': 'http://pdb.virtualflybrain.org',           # production, live VFB2
    'dev': 'http://pdb-dev.virtualflybrain.org',       # dev pipeline, pre-release
    'staging': 'http://pdb-alpha.virtualflybrain.org',  # staging pipeline, pre-release
}
KB_SERVER = 'http://kb.virtualflybrain.org'

# Connectome neurons instance-of >1 symbol-bearing Class. resolvable_by_subclass is true
# when exactly one of the competing classes is a subclass of all the others (so the label
# resolves to that leaf); false means they are not linked in the ontology hierarchy. Also
# returns the raw instance comment/synonyms, which carry the source typing behind the
# ambiguity.
OFFENDER_QUERY = (
    "MATCH (i:Individual:Neuron:has_neuron_connectivity)-[:INSTANCEOF]->(c:Class) "
    "WHERE c.symbol IS NOT NULL AND size(c.symbol) > 0 "
    "WITH i, collect(c) AS cs WHERE size(cs) > 1 "
    "WITH i, cs, [x IN cs WHERE all(o IN cs WHERE o = x OR (x)-[:SUBCLASSOF*1..]->(o))] AS leaf "
    "RETURN i.short_form AS instance_id, i.label AS instance_label, "
    "i.comment AS comment, i.has_exact_synonym AS synonyms, "
    "size(cs) AS n_symbol_classes, (size(leaf) = 1) AS resolvable_by_subclass, "
    "[x IN cs | x.symbol[0] + ' [' + x.short_form + '] ' + x.label] AS symbol_classes, "
    "[x IN cs | x.short_form] AS competing_sfs "
    "ORDER BY resolvable_by_subclass, instance_id"
)

COLUMNS = ['instance_id', 'instance_label', 'n_symbol_classes', 'resolvable_by_subclass',
           'conflict_in_kb', 'symbol_classes', 'kb_typing', 'comment', 'synonyms']


def get_offenders(url):
    nc = neo4j_connect(url, 'neo4j', 'vfb')
    return pd.DataFrame(results_2_dict_list(nc.commit_list([OFFENDER_QUERY])))


def kb_typing(instance_ids):
    """For the given instances, return {instance_id: {'sfs': set(kb INSTANCEOF class
    short_forms), 'typing': 'label [sf] | ...'}} - the neuron's full classification in kb."""
    if not instance_ids:
        return {}
    kb = neo4j_connect(KB_SERVER, 'neo4j', 'vfb')
    query = ("MATCH (i:Individual)-[:INSTANCEOF]->(c:Class) WHERE i.short_form IN %s "
             "RETURN i.short_form AS instance_id, collect(c.short_form) AS sfs, "
             "collect(c.label) AS labels" % list(instance_ids))
    out = {}
    for r in results_2_dict_list(kb.commit_list([query])):
        sfs, labels = r['sfs'], r['labels']
        out[r['instance_id']] = {
            'sfs': set(sfs),
            'typing': ' | '.join('%s [%s]' % (l, s) for s, l in zip(sfs, labels)),
        }
    return out


def fmt_list(value):
    if isinstance(value, list):
        return ' | '.join(str(v) for v in value)
    return '' if value is None else str(value)


def fmt_synonyms(value):
    """has_exact_synonym values are JSON strings like {"annotations":{},"value":"..."}."""
    if not isinstance(value, list):
        return ''
    out = []
    for s in value:
        try:
            out.append(json.loads(s).get('value', s))
        except (ValueError, TypeError):
            out.append(str(s))
    return ' | '.join(out)


def build_report(server_name, url):
    df = get_offenders(url)
    output_file = '../VFB_reporting_results/%s_connectome_multi_symbol_class.tsv' % server_name

    if df.empty:
        pd.DataFrame(columns=COLUMNS).to_csv(output_file, sep='\t', index=False)
        print("INFO: %s - no offenders, wrote empty report to %s" % (server_name, output_file))
        return

    # Cross-check the knowledge base (the curation source): a conflict present in kb
    # (>=2 competing classes) is a source/curation issue; if it is not in kb the extra
    # symbol-bearing class was introduced downstream by the pipeline.
    kb_map = kb_typing(list(df['instance_id']))

    df['conflict_in_kb'] = df.apply(
        lambda r: len(set(r['competing_sfs']) & kb_map.get(r['instance_id'], {}).get('sfs', set())) >= 2,
        axis=1)
    df['kb_typing'] = df['instance_id'].apply(lambda x: kb_map.get(x, {}).get('typing', ''))
    df['symbol_classes'] = df['symbol_classes'].apply(fmt_list)
    df['comment'] = df['comment'].apply(fmt_list)
    df['synonyms'] = df['synonyms'].apply(fmt_synonyms)

    df = df[COLUMNS]
    df.to_csv(output_file, sep='\t', index=False)
    n_kb = int(df['conflict_in_kb'].sum())
    print("INFO: %s - wrote %d rows to %s (%d conflict in kb / source, %d pipeline-introduced)"
          % (server_name, len(df), output_file, n_kb, len(df) - n_kb))


if __name__ == '__main__':
    for name, url in REPORT_SERVERS.items():
        try:
            build_report(name, url)
        except Exception as e:
            print("ERROR: failed to build %s report: %s" % (name, e))
