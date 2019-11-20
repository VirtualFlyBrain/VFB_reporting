from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect
from uk.ac.ebi.vfb.neo4j.neo4j_tools import results_2_dict_list
from owlery_query_tools import OWLeryConnect



def get_lookup(limit_by_prefix=None):
    if limit_by_prefix:
        regex_string = ':.+|'.join(limit_by_prefix) + ':.+'
        where = " AND a.obo_id =~ '%s' " % regex_string
    else:
        where = ''
    nc = neo4j_connect("https://pdb.virtualflybrain.org", "neo4j", "neo4j")
    lookup_query = "MATCH (a:VFB:Class) WHERE exists (a.obo_id)" + where + " RETURN a.obo_id as id, a.label as name"
    q = nc.commit_list([lookup_query])
    r = results_2_dict_list(q)
    lookup = {x['name']: x['id'] for x in r}
    #print(lookup['neuron'])
    property_query = "MATCH (p:Property) WHERE exists(p.obo_id) RETURN p.obo_id as id, p.label as name"
    q = nc.commit_list([property_query])
    r = results_2_dict_list(q)
    lookup.update({x['name']: x['id'] for x in r})
    #print(lookup['neuron'])
    return lookup


def gen_simple_report(terms):
    nc = neo4j_connect("https://pdb.virtualflybrain.org", "neo4j", "neo4j")
    query = """MATCH (n:Class) WHERE n.iri in %s WITH n 
                OPTIONAL MATCH  (n)-[r]->(p:pub) WHERE r.typ = 'syn' 
                WITH n, 
                collect({ synonym: r.synonym, PMID: 'PMID:' + p.PMID, 
                    miniref: p.label}) AS syns 
                OPTIONAL MATCH (n)-[r]-(p:pub) WHERE r.typ = 'def' 
                return n.short_form, n.label, n.description, syns,  
                collect({ PMID: 'PMID:' + p.PMID, miniref: p.label}) as pubs""" % str(terms)
    #print(query)
    q = nc.commit_list([query])
    return results_2_dict_list(q)

def get_terms_by_region(region, cells_only = False, verbose=True):
    """Generate JSON reports for all terms relevant to
     annotating some specific region,
    optionally limited by to cells"""
    oc = OWLeryConnect(lookup=get_lookup(limit_by_prefix=['FBbt']))
    preq = ''
    if cells_only:
        preq = "'cell' that "
    owl_query = preq + "'overlaps' some '%s'" % region
    if verbose:
        print("Running query: %s" % owl_query)
    #print(owl_query)
    terms = oc.get_subclasses(owl_query, query_by_label=True)
    if verbose:
        print("Found: %d terms" % len(terms))
    return gen_simple_report(terms)

def get_subclasses(term,  direct = False):
    """Generate JSOn report of all subclasses of the submitted term."""
    oc = OWLeryConnect(lookup=get_lookup(limit_by_prefix=['FBbt']))
    terms = oc.get_subclasses("'%s'" % term, query_by_label=True)
    return gen_simple_report(terms)






