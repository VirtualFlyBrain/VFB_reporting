from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, results_2_dict_list
import requests
import warnings
import re
import json

## MVP: queries when passed a curie map
## desireable: obo curies automatically generated from query string.

class OWLeryConnect:

    def __init__(self,
                 endpoint="http://owl.virtualflybrain.org/kbs/vfb/",
                 lookup=None,
                 obo_curies=None,
                 curies=None):
        """Endpoint: owlery REST endpoint
           Lookup: Dict of name: ID;
           obo_curies: list of prefixes for generation of OBO curies
           curies: Dict of curie: base"""
        self.owlery_endpoint = endpoint
        if not (lookup):
            self.lookup = {}
        else:
            self.lookup = lookup
        if not curies:
            self.curies = {}
        else:
            self.curies = curies
        if obo_curies:
            self.add_obo_curies(obo_curies)

    def add_obo_curies(self, prefixes):
        obolib = "http://purl.obolibrary.org/obo/"
        c = {p : obolib + p + '_' for p in prefixes}
        self.curies.update(c)

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






