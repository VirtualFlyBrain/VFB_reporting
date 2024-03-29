import unittest
from owlery_query_tools import OWLeryConnect


class OwleryConnectTest(unittest.TestCase):

    def setUp(self):
        self.oc = OWLeryConnect(obo_curies=['FBbt', 'RO', 'BFO'])
        self.test_query = "RO:0002131 some FBbt:00003679"
        self.test_query_labels = "'overlaps' some 'fan-shaped body'"

    def test_labels_2_ids(self):
        self.oc.lookup = {'overlaps': 'RO:0002131',
                          'fan-shaped body': 'FBbt:00003679'}
        self.assertEqual(self.oc.labels_2_ids(self.test_query_labels),
                         self.test_query)

    def test_get_subclasses(self):
        ofb = self.oc.get_subclasses(query=self.test_query)
        self.assertTrue(ofb, "Query failed.")
        self.assertGreater(len(ofb), 150,
                           "Unexpectedly small number structures overlap FB")
        self.oc.lookup = {'overlaps': 'RO:0002131',
                          'fan-shaped body': 'FBbt:00003679'}
        ofbl = self.oc.get_subclasses(query=self.test_query_labels, query_by_label=True)
        self.assertTrue(ofbl, "Query failed.")
        print(len(ofb))
        print(len(ofbl))
        self.assertCountEqual(ofb, ofbl)






if __name__ == '__main__':
    unittest.main()
