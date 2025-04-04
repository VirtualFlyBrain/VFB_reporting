#!/usr/bin/env python
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
import pandas as pd
import numpy as np


def gen_report(server, query, report_name, column_order=None):
    """Generates a pandas dataframe with
    the results of a cypher query against the
    specified server.
    Args:
        server: server connection as [endpoint, usr, pwd]
        query: cypher query
        report_name: df.name
        column_order: optionally specify column order in df."""
    nc = neo4j_connect(*server)
    print(query)
    r = nc.commit_list([query])
    #print(r)
    dc = results_2_dict_list(r)
    report = pd.DataFrame.from_records(dc)
    report.replace(np.nan, '', regex=True, inplace=True)
    report.name = report_name
    if column_order:
        out = report[column_order]
        out.name = report_name
        return out
    else:
        return report


def gen_dataset_report(server,
                       report_name,
                       production_only=False):
    qr = ("MATCH (ds:DataSet) with ds "
          "OPTIONAL MATCH (ds)-[:has_reference]->(p:pub) "
          "WITH ds, p "
          "OPTIONAL MATCH (ds)-[:has_license|license]->(l:License) "
          "WITH ds, p, l "
          "OPTIONAL MATCH (ds)<-[:has_source]-(i:Individual) "
          " RETURN ds.short_form, ds.label, ds.production[0] as `ds.production`, "
          "l.label as license,  p.short_form as pub, "
          "count(i) as individuals order by ds.short_form")
    if production_only:
        qr = qr.replace("MATCH (ds:DataSet) with ds ", "MATCH (ds:DataSet) with ds WHERE ds.production[0] = true ")
    return gen_report(
        server,
        query=qr,
        report_name=report_name, column_order=['ds.short_form',
                                               'ds.label',
                                               'ds.production',
                                               'pub',
                                               'license',
                                               'individuals'])


def gen_dataset_report_prod(server, report_name):
    return gen_report(
        server,
        query=("MATCH (ds:DataSet) WHERE ds.production[0] = true WITH ds "
               "OPTIONAL MATCH (ds)-[:has_reference]->(p:pub) "
               "WITH ds, p "
               "OPTIONAL MATCH (ds)<-[:has_source]-(nb:NBLAST) "
               "WITH ds, p, count(distinct nb) as NBLAST "
               "OPTIONAL MATCH (ds)<-[:has_source]-(nbe:NBLASTexp) "
               "WITH ds, p, NBLAST, count(distinct nbe) as NBLASTexp "
               "OPTIONAL MATCH (:Individual)-[r:overlaps|part_of { pub: p.short_form}]->(:Expression_pattern) "
               "WITH ds, p, NBLAST, NBLASTexp, COUNT (distinct r) as exp_cur "
               "OPTIONAL MATCH (ds)-[]->(l:License) "
               "WITH ds, p, NBLAST, NBLASTexp, exp_cur, l "
               "OPTIONAL MATCH (ds)<-[:has_source]-(i:Individual) "
               "OPTIONAL MATCH (i)-[:INSTANCEOF]->(a:Class) "
               "WITH ds, p, NBLAST, NBLASTexp, exp_cur, count(distinct a) as ontology_terms, l, count(distinct i) as individuals "
               "RETURN ds.short_form, ds.label, "
               "l.label as license, p.short_form as pub, "
               "individuals, NBLAST, NBLASTexp, exp_cur, ontology_terms "
               "order by ds.short_form"),
        report_name=report_name, column_order=['ds.short_form',
                                               'ds.label',
                                               'license',
                                               'pub',
                                               'individuals',
                                               'NBLAST',
                                               'NBLASTexp',
                                               'ontology_terms',
                                               'exp_cur'])


def diff_report(report1: pd.DataFrame, report2: pd.DataFrame):
    """Compare two dataframes. Each dataframe must have a .name attribute.
    Returns a dataframe of rows where there is a difference between the two servers."""
    merged = report1.merge(report2, indicator=True, how='outer')
    merged = merged[merged['_merge'] != 'both']
    merged['server'] = merged['_merge'].map({'left_only': report1.name, 'right_only': report2.name})
    merged.drop('_merge', axis=1, inplace=True)
    merged = merged[[list(merged.columns.values)[0], 'server'] + list(merged.columns.values)[1:-1]]  # column order
    merged.sort_values(['ds.short_form', 'server'], inplace=True)
    return merged


"""
## see Stack Overflow 36891977
left_only = merged[merged['_merge'] == 'left_only']
right_only = merged[merged['_merge'] == 'right_only']
out = {report1.name + '_not_' + report2.name: left_only.drop(columns=['_merge']),
       report2.name + '_not_' + report1.name: right_only.drop(columns=['_merge'])}
return namedtuple('out', out.keys())(*out.values())
"""


def save_report(report, filename):
    report.to_csv(filename, sep='\t', index=False)
