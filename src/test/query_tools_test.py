import unittest
from owlery_query_tools import OWLeryConnect
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect


class OwleryConnectTest(unittest.TestCase):

    def setUp(self):
        self.oc = OWLeryConnect()
        self.nc = neo4j_connect("http://pdb.virtualflybrain.org", "neo4j", "neo4j")
        self.test_query = "'overlaps' some 'fan-shaped body'"


    def test_add_obo_curies(self):
        self.oc.add_obo_curies(['FBbt', 'RO', 'BFO'])
        print(self.oc.curies)

    def test_gen_lookup_from_neo(self):
        self.oc.gen_lookup_from_neo(['FBbt', 'RO', 'BFO'], self.nc)
        print(self.oc.lookup)

    def test_(self):
        self.oc.gen_lookup_from_neo(['FBbt', 'RO', 'BFO'], self.nc)
        self.oc.labels_2_ids(self.test_query)



    def test_(self):
        print(self.oc.get_subclasses(query=self.test_query,
                               query_by_label=True))


if __name__ == '__main__':
    unittest.main()
