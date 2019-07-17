from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
import requests
import warnings
import re
import json

## MVP: queries when passed a curie map
## desireable: obo curies automatically generated from query string.

class OWLeryConnect:

    def __init__(self,
                 owlery_endpoint="http://owl.virtualflybrain.org/kbs/vfb/",
                 lookup=None,
                 obo_prefixes = None):

        if not (lookup):
            self.lookup = {}
        else:
            self.lookup = lookup
        self.owlery_endpoint = owlery_endpoint
        self.curies = {}

    def gen_lookup_from_neo(self, prefixes, nc: neo4j_connect):
        """Prefixes = list of ID prefixes;
        nc = neo4J connection"""
        for p in prefixes:
            # Will only work if Entity covers properties - currently does not
            query = "MATCH (e:Entity) WHERE e.short_form =~ '%s_.+' " \
                    "RETURN e.label as label, e.obo_id as obo_id" % p
            q = nc.commit_list([query])
            if q:
                result = results_2_dict_list(q)
                lookup = {x['label']: x['obo_id'] for x in result}
                self.lookup.update(lookup)

    def extend_curies(self, curies):
        self.curies.update(curies)

    def add_obo_curies(self, prefixes):
        obolib = "http://purl.obolibrary.org/obo/"
        c = { p : obolib + p for p in prefixes}
        self.extend_curies(c)

    def get_subclasses(self, query, query_by_label=False):
        owl_endpoint = self.owlery_endpoint + "subclasses?"
        if query_by_label:
            query = self.labels_2_ids(query)
        payload = {'object': query, 'prefixes': json.dumps(self.curies), 'direct': False}
        r = requests.get(url=owl_endpoint, params=payload)
        if r.status_code == 200:
            return r.json()['superClassOf']
        else:
            warnings.warn(str(r.content))
            return False

    def labels_2_ids(self, query_string):
        """Substitutes labels for IDs in a query string"""
        return re.sub(r"'(.+?)'", lambda m: self.lookup.get(m.group(1)), query_string)








