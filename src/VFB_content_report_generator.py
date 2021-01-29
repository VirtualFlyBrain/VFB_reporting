from reporting_tools import gen_report
import mdutils
import datetime

output_file = "../VFB_reporting_results/content_report.md"
VFB_server = ('http://pdb.virtualflybrain.org', 'neo4j', 'neo4j')


class VFBContentReport:
    """Class for storing data about the amount of content in VFB."""
    def __init__(self, server):
        self.server = server
        self.timestamp = None
        self.total_neuron_number = None
        self.total_neuron_pub_number = None
        self.provisional_neuron_number = None
        self.provisional_neuron_pub_number = None
        self.characterised_neuron_number = None
        self.characterised_neuron_pub_number = None
        self.all_region_number = None
        self.all_region_pub_number = None
        self.synaptic_neuropil_number = None
        self.synaptic_neuropil_pub_number = None
        self.neuron_projection_bundle_number = None
        self.neuron_projection_bundle_pub_number = None
        self.cell_body_rind_number = None
        self.cell_body_rind_pub_number = None

    def get_info(self):
        """Gets content info from VFB and assigns to attributes."""
        self.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)

        # numbers of neurons
        all_neurons = gen_report(server=self.server,
                                 query=("MATCH (c:Class:Neuron) "
                                        "WHERE c.short_form =~ 'FBbt.+' "
                                        "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                        "RETURN count(distinct c) as neurons, "
                                        "count (distinct p) as pubs"),
                                 report_name='all_neurons')

        provisional_neurons = gen_report(server=self.server,
                                         query=("MATCH (c:Class:Neuron) "
                                                "where c.short_form =~ 'FBbt[_]2.+' "
                                                "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                                "RETURN count(distinct c) as neurons, "
                                                "count (distinct p) as pubs"),
                                         report_name='provisional_neurons')

        characterised_neurons = gen_report(server=self.server,
                                           query=("MATCH (c:Class:Neuron) "
                                                  "WHERE NOT c.short_form =~ 'FBbt[_]2.+' "
                                                  "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                                  "RETURN count(distinct c) as neurons, "
                                                  "count (distinct p) as pubs"),
                                           report_name='characterized_neurons')

        self.total_neuron_number = all_neurons['neurons'][0]
        self.total_neuron_pub_number = all_neurons['pubs'][0]
        self.provisional_neuron_number = provisional_neurons['neurons'][0]
        self.provisional_neuron_pub_number = provisional_neurons['pubs'][0]
        self.characterised_neuron_number = characterised_neurons['neurons'][0]
        self.characterised_neuron_pub_number = characterised_neurons['pubs'][0]

        # numbers of regions
        synaptic_neuropils = gen_report(server=self.server,
                                        query=("MATCH (c:Synaptic_neuropil) "
                                               "WHERE c.short_form =~ 'FBbt.+' "
                                               "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                               "RETURN count(distinct c) as regions, "
                                               "count (distinct p) as pubs"),
                                        report_name='synaptic_neuropils')

        neuron_projection_bundles = gen_report(server=self.server,
                                               query=("MATCH (c:Neuron_projection_bundle) "
                                                      "WHERE c.short_form =~ 'FBbt.+' "
                                                      "WITH c OPTIONAL MATCH "
                                                      "(c)-[]->(p:pub) "
                                                      "RETURN count(distinct c) "
                                                      "as regions, "
                                                      "count (distinct p) as pubs"),
                                               report_name='neuron_projection_bundles')

        cell_body_rinds = gen_report(server=self.server,
                                     query=("MATCH (c:Class)-[:SUBCLASSOF*1..2]"
                                            "->(b:Class) "
                                            "WHERE c.short_form =~ 'FBbt.+' "
                                            "AND b.short_form = 'FBbt_00100200'"
                                            "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                            "RETURN count(distinct c) as regions, "
                                            "count (distinct p) as pubs"),
                                     report_name='cell_body_rinds')

        all_regions = gen_report(server=self.server,
                                 query=("MATCH (c:Class) "
                                        "WHERE c.short_form =~ 'FBbt.+' "
                                        "AND (ANY(x IN ['Synaptic_neuropil', "
                                        "'Neuron_projection_bundle', 'Ganglion', "
                                        "'Neuromere'] WHERE x in labels(c)) "
                                        "OR ((c)-[:SUBCLASSOF]->"
                                        "(:Class {short_form:'FBbt_00100200'}))) "
                                        "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                        "RETURN count(distinct c) as regions, "
                                        "count(distinct p) as pubs"),
                                 report_name='other_regions')

        self.all_region_number = all_regions['regions'][0]
        self.all_region_pub_number = all_regions['pubs'][0]
        self.synaptic_neuropil_number = synaptic_neuropils['regions'][0]
        self.synaptic_neuropil_pub_number = synaptic_neuropils['pubs'][0]
        self.neuron_projection_bundle_number = neuron_projection_bundles['regions'][0]
        self.neuron_projection_bundle_pub_number = neuron_projection_bundles['pubs'][0]
        self.cell_body_rind_number = cell_body_rinds['regions'][0]
        self.cell_body_rind_pub_number = cell_body_rinds['pubs'][0]


    def prepare_report(self, filename):
        """Put content data into an output file"""
        f = mdutils.MdUtils(file_name=filename, title='VFB Content Report ' +
                                                      self.timestamp.date().isoformat())
        f.new_paragraph("Report of content found on ``%s`` as at ``%s``"
                        % (self.server[0],
                           self.timestamp.strftime("%a, %d %b %Y %H:%M:%S")))
        # table for ontology content
        f.new_line()
        table_content = ['Anatomy', 'Classes', 'Publications']
        table_content.extend(['All Neurons',
                              str(self.total_neuron_number),
                              str(self.total_neuron_pub_number)])
        table_content.extend(['Characterised Neurons',
                              str(self.characterised_neuron_number),
                              str(self.characterised_neuron_pub_number)])
        table_content.extend(['Provisional Neurons',
                              str(self.provisional_neuron_number),
                              str(self.provisional_neuron_pub_number)])
        table_content.extend(['All Regions',
                              str(self.all_region_number),
                              str(self.all_region_pub_number)])
        table_content.extend(['Synaptic Neuropils',
                              str(self.synaptic_neuropil_number),
                              str(self.synaptic_neuropil_pub_number)])
        table_content.extend(['Neuron Projection Bundles',
                              str(self.neuron_projection_bundle_number),
                              str(self.neuron_projection_bundle_pub_number)])
        table_content.extend(['Cell Body Rinds',
                              str(self.cell_body_rind_number),
                              str(self.cell_body_rind_pub_number)])
        print(table_content)
        f.new_table(columns=3, rows=8, text=table_content, text_align='left')
        f.create_md_file()


report = VFBContentReport(server=VFB_server)
report.get_info()
report.prepare_report(filename=output_file)

