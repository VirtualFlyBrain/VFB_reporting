import unittest
from owlery_query_tools import OWLeryConnect


class OwleryConnectTest(unittest.TestCase):

    def setUp(self):
        self.oc = OWLeryConnect(obo_curies=['FBbt', 'RO', 'BFO'])
        self.test_query = "RO:0002131 some FBbt:00003679"

    def test_get_subclasses(self):
        ofb = self.oc.get_subclasses(query=self.test_query)
        self.assertTrue(ofb, "Query failed.")
        self.assertGreater(len(ofb), 150,
                           "Unexpectedly small number structures overlap FB")

if __name__ == '__main__':
    unittest.main()
