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
        self.all_terms_number = None
        self.all_terms_pubs = None
        self.all_nervous_system_number = None
        self.all_nervous_system_pubs = None
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
        self.sense_organ_number = None
        self.sense_organ_pubs = None
        self.non_isa_relationship_number = None
        self.isa_relationship_number = None
        self.all_relationship_number = None
        self.ns_non_isa_relationship_number = None
        self.ns_isa_relationship_number = None
        self.ns_all_relationship_number = None
        self.all_image_number = None
        self.all_image_ds_number = None
        self.single_neuron_image_number = None
        self.single_neuron_image_type_number = None
        self.exp_pattern_number = None
        self.split_exp_pattern_driver_number = None
        self.split_image_number = None
        self.split_image_driver_number = None
        self.split_neuron_annotations_split_number = None
        self.split_neuron_annotations_annotation_number = None
        self.split_neuron_annotations_neuron_number = None
        self.driver_anatomy_annotations_EP_number = None
        self.driver_anatomy_annotations_annotation_number = None
        self.driver_anatomy_annotations_anatomy_number = None
        self.driver_neuron_annotations_EP_number = None
        self.driver_neuron_annotations_annotation_number = None
        self.driver_neuron_annotations_neuron_number = None
        self.neuron_connections_neuron_number = None
        self.neuron_connections_connection_number = None
        self.region_connections_neuron_number = None
        self.region_connections_region_number = None
        self.region_connections_connection_number = None
        self.muscle_connections_neuron_number = None
        self.muscle_connections_muscle_number = None
        self.muscle_connections_connection_number = None
        self.sensory_connections_neuron_number = None
        self.sensory_connections_sense_organ_number = None
        self.sensory_connections_connection_number = None

    def get_info(self):
        """Gets content info from VFB and assigns to attributes."""
        self.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)

        # all terms
        all_terms = gen_report(server=self.server,
                               query=("MATCH (c:Class) "
                                      "WHERE c.short_form =~ 'FBbt.+' "
                                      "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                      "RETURN count(distinct c) as parts, "
                                      "count (distinct p) as pubs"),
                               report_name='all_terms')
        self.all_terms_number = all_terms['parts'][0]
        self.all_terms_pubs = all_terms['pubs'][0]

        # total nervous system parts
        all_nervous_system = gen_report(server=self.server,
                                        query=("MATCH (c:Nervous_system) "
                                               "WHERE c.short_form =~ 'FBbt.+' "
                                               "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                               "RETURN count(distinct c) as parts, "
                                               "count (distinct p) as pubs"),
                                        report_name='all_nervous_system')

        self.all_nervous_system_number = all_nervous_system['parts'][0]
        self.all_nervous_system_pubs = all_nervous_system['pubs'][0]

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

        # sense organs
        sense_organs = gen_report(server=self.server,
                                  query=("MATCH (c:Class)-[:SUBCLASSOF*]"
                                         "->(b:Class) "
                                         "WHERE c.short_form =~ 'FBbt.+' "
                                         "AND b.short_form = 'FBbt_00005155'"
                                         "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                         "RETURN count(distinct c) as types, "
                                         "count (distinct p) as pubs"),
                                  report_name='sense_organs')

        self.sense_organ_number = sense_organs['types'][0]
        self.sense_organ_pubs = sense_organs['pubs'][0]

        # relationships in ontology
        non_isa_relationships = gen_report(server=self.server,
                                           query=("MATCH (c:Class)-[r]->(d:Class) where c.short_form =~ 'FBbt.+'"
                                                  " and (r.type = 'Related') return count (distinct r) as total"),
                                           report_name='non_isa_relationships')

        isa_relationships = gen_report(server=self.server,
                                       query=("MATCH (c:Class)-[r]->(d:Class) where c.short_form =~ 'FBbt.+' "
                                              "and (type(r) = 'SUBCLASSOF') return count (distinct r) as total"),
                                       report_name='isa_relationships')

        all_relationships = gen_report(server=self.server,
                                       query=("MATCH (c:Class)-[r]->(d:Class) where c.short_form =~ 'FBbt.+' "
                                              "and ((r.type = 'Related') OR (type(r) = 'SUBCLASSOF')) "
                                              "return count (distinct r) as total"),
                                       report_name='all_relationships')

        self.non_isa_relationship_number = non_isa_relationships['total'][0]
        self.isa_relationship_number = isa_relationships['total'][0]
        self.all_relationship_number = all_relationships['total'][0]

        # nervous system relationships in ontology
        ns_non_isa_relationships = gen_report(server=self.server,
                                              query=(
                                                  "MATCH (c:Nervous_system)-[r]->(d:Class) "
                                                  "where c.short_form =~ 'FBbt.+'"
                                                  " and (r.type = 'Related') return count (distinct r) as total"),
                                              report_name='non_isa_relationships')

        ns_isa_relationships = gen_report(server=self.server,
                                          query=(
                                              "MATCH (c:Nervous_system)-[r]->(d:Class) where c.short_form =~ 'FBbt.+' "
                                              "and (type(r) = 'SUBCLASSOF') return count (distinct r) as total"),
                                          report_name='isa_relationships')

        ns_all_relationships = gen_report(server=self.server,
                                          query=(
                                              "MATCH (c:Nervous_system)-[r]->(d:Class) where c.short_form =~ 'FBbt.+' "
                                              "and ((r.type = 'Related') OR (type(r) = 'SUBCLASSOF')) "
                                              "return count (distinct r) as total"),
                                          report_name='all_relationships')

        self.ns_non_isa_relationship_number = ns_non_isa_relationships['total'][0]
        self.ns_isa_relationship_number = ns_isa_relationships['total'][0]
        self.ns_all_relationship_number = ns_all_relationships['total'][0]

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

        # annotations

        split_neuron_annotations = gen_report(server=self.server,
                                              query=("MATCH p=(split:Class:Split)<-[r:part_of]-(j:Individual)-"
                                                     "[:INSTANCEOF]->(n:Neuron:Class) WHERE exists(r.pub) "
                                                     "RETURN count(distinct split) AS Splits, count(r) AS "
                                                     "annotations, count(distinct n) as neurons"),
                                              report_name='split_neuron_annotations')

        driver_anatomy_annotations = gen_report(server=self.server,
                                                query=("MATCH p=(ep:Class:Expression_pattern)<-"
                                                       "[r:part_of|overlaps]-(j:Individual)-[:INSTANCEOF]->(n:Class) "
                                                       "WHERE exists(r.pub) "
                                                       "RETURN count(distinct ep) AS EPs, count(r) AS annotations, "
                                                       "count(distinct n) as anatomy"),
                                                report_name='driver_anatomy_annotations')

        driver_neuron_annotations = gen_report(server=self.server,
                                               query=("MATCH p=(ep:Class:Expression_pattern)<-[r:part_of]-"
                                                      "(j:Individual)-[:INSTANCEOF]->(n:Neuron:Class) "
                                                      "WHERE exists(r.pub) "
                                                      "RETURN count(distinct ep) AS EPs, count(r) AS annotations, "
                                                      "count(distinct n) as neurons"),
                                               report_name='driver_neuron_annotations')

        self.split_neuron_annotations_split_number = split_neuron_annotations['Splits'][0]
        self.split_neuron_annotations_annotation_number = split_neuron_annotations['annotations'][0]
        self.split_neuron_annotations_neuron_number = split_neuron_annotations['neurons'][0]
        self.driver_anatomy_annotations_EP_number = driver_anatomy_annotations['EPs'][0]
        self.driver_anatomy_annotations_annotation_number = driver_anatomy_annotations['annotations'][0]
        self.driver_anatomy_annotations_anatomy_number = driver_anatomy_annotations['anatomy'][0]
        self.driver_neuron_annotations_EP_number = driver_neuron_annotations['EPs'][0]
        self.driver_neuron_annotations_annotation_number = driver_neuron_annotations['annotations'][0]
        self.driver_neuron_annotations_neuron_number = driver_neuron_annotations['neurons'][0]

        # connectivity

        neuron_connections = gen_report(server=self.server,
                                               query=("MATCH (i:Individual:Neuron)-[r:synapsed_to]->"
                                                      "(j:Individual:Neuron) "
                                                      "WITH collect(r) AS rels, collect(distinct i) AS ci, "
                                                      "collect(distinct j) AS cj "
                                                      "RETURN size(apoc.coll.union(ci,cj)) AS neurons, "
                                                      "size(rels) AS connections"),
                                               report_name='synaptic_connections')

        self.neuron_connections_neuron_number = neuron_connections['neurons'][0]
        self.neuron_connections_connection_number = neuron_connections['connections'][0]

        region_connections = gen_report(server=self.server,
                                        query=("MATCH (n:Individual:Neuron)-"
                                               "[r:has_presynaptic_terminals_in|:has_postsynaptic_terminal_in]"
                                               "->(m:Individual) "
                                               "RETURN count(distinct n) AS neurons, count(distinct m) AS regions, "
                                               "count(distinct r) AS connections"),
                                        report_name='synaptic_connections')

        self.region_connections_neuron_number = region_connections['neurons'][0]
        self.region_connections_region_number = region_connections['regions'][0]
        self.region_connections_connection_number = region_connections['connections'][0]

        muscle_connections = gen_report(server=self.server,
                                               query=("MATCH (n:Neuron)-[r:synapsed_to|"
                                                      ":synapsed_via_type_Is_bouton_to|"
                                                      ":synapsed_via_type_Ib_bouton_to|"
                                                      ":synapsed_via_type_II_bouton_to|"
                                                      ":synapsed_via_type_III_bouton_to]->(m:Muscle) "
                                                      "WITH n, r, m OPTIONAL MATCH (n2:Neuron)-[:SUBCLASSOF*]->(n) "
                                                      "WITH collect(distinct r) AS rels, collect(distinct n) AS cn, "
                                                      "collect(distinct n2) AS cn2, collect(distinct m) AS cm "
                                                      "RETURN size(apoc.coll.union(cn,cn2)) AS neurons, "
                                                      "size(cm) AS muscles, size(rels) AS connections"),
                                               report_name='muscle_connections')

        self.muscle_connections_neuron_number = muscle_connections['neurons'][0]
        self.muscle_connections_muscle_number = muscle_connections['muscles'][0]
        self.muscle_connections_connection_number = muscle_connections['connections'][0]

        sensory_connections = gen_report(server=self.server,
                                        query=("MATCH (n:Neuron)-[r:has_sensory_dendrite_in]->(s:Sense_organ) "
                                               "WITH n, r, s OPTIONAL MATCH (n2:Neuron)-[:SUBCLASSOF*]->(n) "
                                               "WITH collect(r) AS rels, collect(distinct n) AS cn, "
                                               "collect(distinct n2) AS cn2, collect(distinct s) AS cs "
                                               "RETURN size(apoc.coll.union(cn,cn2)) AS neurons, "
                                               "size(cs) AS sense_organs, size(rels) AS connections"),
                                        report_name='sensory_connections')

        self.sensory_connections_neuron_number = sensory_connections['neurons'][0]
        self.sensory_connections_sense_organ_number = sensory_connections['sense_organs'][0]
        self.sensory_connections_connection_number = sensory_connections['connections'][0]

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
        anatomy_table_content.extend(['All Terms',
                                      str(self.all_terms_number),
                                      str(self.all_terms_pubs)])
        anatomy_table_content.extend(['All Nervous System Parts',
                                      str(self.all_nervous_system_number),
                                      str(self.all_nervous_system_pubs)])
        anatomy_table_content.extend(['All Neurons',
                                      str(self.total_neuron_number),
                                      str(self.total_neuron_pub_number)])
        anatomy_table_content.extend(['Characterised Neurons',
                                      str(self.characterised_neuron_number),
                                      str(self.characterised_neuron_pub_number)])
        anatomy_table_content.extend(['Provisional Neurons',
                                      str(self.provisional_neuron_number),
                                      str(self.provisional_neuron_pub_number)])
        anatomy_table_content.extend(['All Nervous System Regions',
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
        anatomy_table_content.extend(['Sense Organs',
                                      str(self.sense_organ_number),
                                      str(self.sense_organ_pubs)])
        f.new_table(columns=3, rows=11, text=anatomy_table_content, text_align='left')
        f.new_line()
        f.new_line('**%s** formal assertions, of which **%s** are SubClassOf assertions and **%s** are '
                   'other relationship types'
                   % (str(self.all_relationship_number),
                      str(self.isa_relationship_number),
                      str(self.non_isa_relationship_number)))
        f.new_line()
        f.new_line('**%s** formal assertions on nervous system components, of which **%s** are SubClassOf '
                   'assertions and **%s** are other relationship types'
                   % (str(self.ns_all_relationship_number),
                      str(self.ns_isa_relationship_number),
                      str(self.ns_non_isa_relationship_number)))

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

        f.new_line()
        f.new_line("Annotations", bold_italics_code='bic')
        f.new_line()
        f.new_line('**%s** annotations recording **%s** types of neurons that **%s** specific '
                   'split combinations are expressed in.'
                   % (str(self.split_neuron_annotations_annotation_number),
                      str(self.split_neuron_annotations_neuron_number),
                      str(self.split_neuron_annotations_split_number)))
        f.new_line('**%s** annotations recording **%s** types of anatomical structures that '
                   '**%s** specific driver lines are expressed in.'
                   % (str(self.driver_anatomy_annotations_annotation_number),
                      str(self.driver_anatomy_annotations_anatomy_number),
                      str(self.driver_anatomy_annotations_EP_number)))
        f.new_line('**%s** annotations recording **%s** types of neurons that '
                   '**%s** specific driver lines are expressed in.'
                   % (str(self.driver_neuron_annotations_annotation_number),
                      str(self.driver_neuron_annotations_neuron_number),
                      str(self.driver_neuron_annotations_EP_number)))

        f.new_line()
        f.new_line("Connectivity", bold_italics_code='bic')

        f.new_line()
        connectivity_table_content = ['Neuron', 'Number of Neurons', 'Input/Output Entity',
                                      'Number of Entities', 'Connections']
        connectivity_table_content.extend(['Any neuron (individuals)', str(self.neuron_connections_neuron_number),
                                           'Any neuron (individuals)', str(self.neuron_connections_neuron_number),
                                           str(self.neuron_connections_connection_number)])
        connectivity_table_content.extend(['Any neuron (individuals)', str(self.region_connections_neuron_number),
                                           'Region (individuals)', str(self.region_connections_region_number),
                                           str(self.region_connections_connection_number)])
        connectivity_table_content.extend(['Any neuron (classes)', str(self.muscle_connections_neuron_number),
                                           'Muscle (classes)', str(self.muscle_connections_muscle_number),
                                           str(self.muscle_connections_connection_number)])
        connectivity_table_content.extend(['Any neuron (classes)', str(self.sensory_connections_neuron_number),
                                           'Sense organ (classes)', str(self.sensory_connections_sense_organ_number),
                                           str(self.sensory_connections_connection_number)])

        f.new_table(columns=5, rows=5, text=connectivity_table_content, text_align='left')
        f.new_line()

        f.create_md_file()


report = VFBContentReport(server=VFB_server)
report.get_info()
report.prepare_report(filename=output_file)
