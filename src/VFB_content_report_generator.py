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
        self.all_image_number = None
        self.all_image_ds_number = None
        self.single_neuron_image_number = None
        self.single_neuron_image_type_number = None
        self.exp_pattern_number = None
        self.split_exp_pattern_driver_number = None
        self.split_image_number = None
        self.split_image_driver_number = None

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

        # images
        all_images = gen_report(server=self.server,
                                query=("MATCH (i:Individual)-[]->(n:DataSet) "
                                       "WHERE n.production "
                                       "RETURN count(distinct i) as images, "
                                       "count(distinct n) as ds"),
                                report_name='all_images')

        single_neuron_images = gen_report(server=self.server,
                                          query=("MATCH (n:DataSet)<-[]-"
                                                 "(i:Individual:Neuron)-"
                                                 "[:INSTANCEOF]->(c:Class) "
                                                 "WHERE n.production "
                                                 "RETURN count(distinct i) as images, "
                                                 "count(distinct c) as types"),
                                          report_name='single_neuron_images')

        exp_pattern_images = gen_report(server=self.server,
                                        query=("MATCH (n:DataSet)<-[]-"
                                               "(i:Individual:Expression_pattern)-"
                                               "[:INSTANCEOF]->(c:Class) "
                                               "WHERE n.production "
                                               "RETURN count(distinct i) as images, "
                                               "count(distinct c) as drivers"),
                                        report_name='exp_pattern_images')

        split_images = gen_report(server=self.server,
                                  query=("MATCH (n:DataSet)<-[]-(i:Split)-"
                                         "[:INSTANCEOF]->(c:Class) "
                                         "WHERE n.production "
                                         "RETURN count(distinct i) as images, "
                                         "count(distinct c) as split_classes"),
                                  report_name='split_images')

        self.all_image_number = all_images['images'][0]
        self.all_image_ds_number = all_images['ds'][0]
        self.single_neuron_image_number = single_neuron_images['images'][0]
        self.single_neuron_image_type_number = single_neuron_images['types'][0]
        self.exp_pattern_number = exp_pattern_images['images'][0]
        self.split_exp_pattern_driver_number = exp_pattern_images['drivers'][0]
        self.split_image_number = split_images['images'][0]
        self.split_image_driver_number = split_images['split_classes'][0]

    def prepare_report(self, filename):
        """Put content data into an output file"""
        f = mdutils.MdUtils(file_name=filename, title='VFB Content Report ' +
                                                      self.timestamp.date().isoformat())
        f.new_paragraph("Report of content found at ``%s`` on ``%s``"
                        % (self.server[0],
                           self.timestamp.strftime("%a, %d %b %Y %H:%M:%S")))
        f.new_line()
        f.new_line("Ontology Content", bold_italics_code='bic')
        f.new_line()
        anatomy_table_content = ['Anatomy', 'Classes', 'Publications']
        anatomy_table_content.extend(['All Neurons',
                                      str(self.total_neuron_number),
                                      str(self.total_neuron_pub_number)])
        anatomy_table_content.extend(['Characterised Neurons',
                                      str(self.characterised_neuron_number),
                                      str(self.characterised_neuron_pub_number)])
        anatomy_table_content.extend(['Provisional Neurons',
                                      str(self.provisional_neuron_number),
                                      str(self.provisional_neuron_pub_number)])
        anatomy_table_content.extend(['All Regions',
                                      str(self.all_region_number),
                                      str(self.all_region_pub_number)])
        anatomy_table_content.extend(['Synaptic Neuropils',
                                      str(self.synaptic_neuropil_number),
                                      str(self.synaptic_neuropil_pub_number)])
        anatomy_table_content.extend(['Neuron Projection Bundles',
                                      str(self.neuron_projection_bundle_number),
                                      str(self.neuron_projection_bundle_pub_number)])
        anatomy_table_content.extend(['Cell Body Rinds',
                                      str(self.cell_body_rind_number),
                                      str(self.cell_body_rind_pub_number)])
        f.new_table(columns=3, rows=8, text=anatomy_table_content, text_align='left')

        f.new_line()
        f.new_line("Image Content", bold_italics_code='bic')
        f.new_line()
        f.new_line('**%s** total images from **%s** datasets'
                   % (str(self.all_image_number),
                      str(self.all_image_ds_number)))
        f.new_line('**%s** single neuron images of **%s** cell types'
                   % (str(self.single_neuron_image_number),
                      str(self.single_neuron_image_type_number)))
        f.new_line('**%s** images of expression patterns of **%s** drivers'
                   % (str(self.exp_pattern_number),
                      str(self.split_exp_pattern_driver_number)))
        f.new_line('**%s** images of expression patterns of **%s** split combinations'
                   % (str(self.split_image_number),
                      str(self.split_image_driver_number)))

        f.create_md_file()


report = VFBContentReport(server=VFB_server)
report.get_info()
report.prepare_report(filename=output_file)
