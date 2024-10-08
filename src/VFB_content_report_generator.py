from reporting_tools import gen_report
import mdutils
import datetime

VFB_servers = {'pdb': ('http://pdb.virtualflybrain.org', 'neo4j', 'vfb'),
               'pdb-alpha': ('http://pdb-alpha.virtualflybrain.org', 'neo4j', 'vfb'),
               'pdb-preview': ('http://pdb.ug.virtualflybrain.org', 'neo4j', 'vfb')}
output_files = {'pdb': "../VFB_reporting_results/content_report.md",
                'pdb-alpha': "../VFB_reporting_results/content_report_alpha.md",
                'pdb-preview': "../VFB_reporting_results/content_report_preview.md"}

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
        self.exp_pattern_fragment_number = None
        self.split_exp_pattern_fragment_driver_number = None
        self.driver_anatomy_annotations_EP_number = None
        self.driver_anatomy_annotations_annotation_number = None
        self.driver_anatomy_annotations_anatomy_number = None
        self.driver_ns_annotations_EP_number = None
        self.driver_ns_annotations_annotation_number = None
        self.driver_ns_annotations_anatomy_number = None
        self.driver_neuron_annotations_EP_number = None
        self.driver_neuron_annotations_annotation_number = None
        self.driver_neuron_annotations_neuron_number = None
        self.split_neuron_annotations_split_number = None
        self.split_neuron_annotations_annotation_number = None
        self.split_neuron_annotations_neuron_number = None
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
        self.templates_data = None

    def get_info(self):
        """Gets content info from VFB and assigns to attributes."""
        self.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)

        def safe_gen_report(*args, **kwargs):
            try:
                return gen_report(*args, **kwargs)
            except Exception as e:
                print(f"Error generating report {kwargs.get('report_name')}: {e}")
                return None

        # all terms
        all_terms = safe_gen_report(server=self.server,
                               query=("MATCH (c:Class) "
                                      "WHERE c.short_form =~ 'FBbt.+' "
                                      "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                      "RETURN COUNT(DISTINCT c) AS parts, "
                                      "COUNT(DISTINCT p) AS pubs"),
                               report_name='all_terms')
        if all_terms is not None:
              self.all_terms_number = all_terms['parts'][0]
              self.all_terms_pubs = all_terms['pubs'][0]

        # total nervous system parts
        all_nervous_system = safe_gen_report(server=self.server,
                                        query=("MATCH (c:Nervous_system) "
                                               "WHERE c.short_form =~ 'FBbt.+' "
                                               "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                               "RETURN COUNT(DISTINCT c) AS parts, "
                                               "COUNT(DISTINCT p) AS pubs"),
                                        report_name='all_nervous_system')
        if all_nervous_system is not None:
              self.all_nervous_system_number = all_nervous_system['parts'][0]
              self.all_nervous_system_pubs = all_nervous_system['pubs'][0]

        # numbers of neurons
        all_neurons = safe_gen_report(server=self.server,
                                 query=("MATCH (c:Class:Neuron) "
                                        "WHERE c.short_form =~ 'FBbt.+' "
                                        "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                        "RETURN COUNT(DISTINCT c) AS neurons, "
                                        "COUNT(DISTINCT p) AS pubs"),
                                 report_name='all_neurons')

        provisional_neurons = safe_gen_report(server=self.server,
                                         query=("MATCH (c:Class:Neuron) "
                                                "WHERE c.short_form =~ 'FBbt[_]2.+' "
                                                "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                                "RETURN COUNT(DISTINCT c) AS neurons, "
                                                "COUNT(DISTINCT p) AS pubs"),
                                         report_name='provisional_neurons')

        characterised_neurons = safe_gen_report(server=self.server,
                                           query=("MATCH (c:Class:Neuron) "
                                                  "WHERE NOT c.short_form =~ 'FBbt[_]2.+' "
                                                  "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                                  "RETURN COUNT(DISTINCT c) AS neurons, "
                                                  "COUNT(DISTINCT p) AS pubs"),
                                           report_name='characterized_neurons')
        if all_neurons is not None:
              self.total_neuron_number = all_neurons['neurons'][0]
              self.total_neuron_pub_number = all_neurons['pubs'][0]
              self.provisional_neuron_number = provisional_neurons['neurons'][0]
              self.provisional_neuron_pub_number = provisional_neurons['pubs'][0]
              self.characterised_neuron_number = characterised_neurons['neurons'][0]
              self.characterised_neuron_pub_number = characterised_neurons['pubs'][0]

        # numbers of regions
        synaptic_neuropils = safe_gen_report(server=self.server,
                                        query=("MATCH (c:Synaptic_neuropil) "
                                               "WHERE c.short_form =~ 'FBbt.+' "
                                               "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                               "RETURN COUNT(DISTINCT c) AS regions, "
                                               "COUNT(DISTINCT p) AS pubs"),
                                        report_name='synaptic_neuropils')

        neuron_projection_bundles = safe_gen_report(server=self.server,
                                               query=("MATCH (c:Neuron_projection_bundle) "
                                                      "WHERE c.short_form =~ 'FBbt.+' "
                                                      "WITH c OPTIONAL MATCH "
                                                      "(c)-[]->(p:pub) "
                                                      "RETURN COUNT(DISTINCT c) "
                                                      "AS regions, "
                                                      "COUNT(DISTINCT p) AS pubs"),
                                               report_name='neuron_projection_bundles')

        cell_body_rinds = safe_gen_report(server=self.server,
                                     query=("MATCH (c:Class)-[:SUBCLASSOF*1..2]"
                                            "->(b:Class) "
                                            "WHERE c.short_form =~ 'FBbt.+' "
                                            "AND b.short_form = 'FBbt_00100200' "
                                            "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                            "RETURN COUNT(DISTINCT c) AS regions, "
                                            "COUNT(DISTINCT p) AS pubs"),
                                     report_name='cell_body_rinds')

        all_regions = safe_gen_report(server=self.server,
                                 query=("MATCH (c:Class) "
                                        "WHERE c.short_form =~ 'FBbt.+' "
                                        "AND (ANY(x IN ['Synaptic_neuropil', "
                                        "'Neuron_projection_bundle', 'Ganglion', "
                                        "'Neuromere'] WHERE x in labels(c)) "
                                        "OR ((c)-[:SUBCLASSOF]->"
                                        "(:Class {short_form:'FBbt_00100200'}))) "
                                        "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                        "RETURN COUNT(DISTINCT c) AS regions, "
                                        "COUNT(DISTINCT p) AS pubs"),
                                 report_name='other_regions')
        if all_regions is not None:
              self.all_region_number = all_regions['regions'][0]
              self.all_region_pub_number = all_regions['pubs'][0]
              self.synaptic_neuropil_number = synaptic_neuropils['regions'][0]
              self.synaptic_neuropil_pub_number = synaptic_neuropils['pubs'][0]
              self.neuron_projection_bundle_number = neuron_projection_bundles['regions'][0]
              self.neuron_projection_bundle_pub_number = neuron_projection_bundles['pubs'][0]
              self.cell_body_rind_number = cell_body_rinds['regions'][0]
              self.cell_body_rind_pub_number = cell_body_rinds['pubs'][0]

        # sense organs
        sense_organs = safe_gen_report(server=self.server,
                                  query=("MATCH (c:Class)-[:SUBCLASSOF*]"
                                         "->(b:Class) "
                                         "WHERE c.short_form =~ 'FBbt.+' "
                                         "AND b.short_form = 'FBbt_00005155' "
                                         "WITH c OPTIONAL MATCH (c)-[]->(p:pub) "
                                         "RETURN COUNT(DISTINCT c) AS types, "
                                         "COUNT(DISTINCT p) AS pubs"),
                                  report_name='sense_organs')
        if sense_organs is not None:
              self.sense_organ_number = sense_organs['types'][0]
              self.sense_organ_pubs = sense_organs['pubs'][0]

        # relationships in ontology
        non_isa_relationships = safe_gen_report(server=self.server,
                                           query=("MATCH (c:Class)-[r]->(d:Class) WHERE c.short_form =~ 'FBbt.+' "
                                                  "AND (r.type = 'Related') RETURN COUNT(DISTINCT r) AS total"),
                                           report_name='non_isa_relationships')

        isa_relationships = safe_gen_report(server=self.server,
                                       query=("MATCH (c:Class)-[r]->(d:Class) WHERE c.short_form =~ 'FBbt.+' "
                                              "AND (type(r) = 'SUBCLASSOF') RETURN COUNT(DISTINCT r) AS total"),
                                       report_name='isa_relationships')

        all_relationships = safe_gen_report(server=self.server,
                                       query=("MATCH (c:Class)-[r]->(d:Class) WHERE c.short_form =~ 'FBbt.+' "
                                              "AND ((r.type = 'Related') OR (type(r) = 'SUBCLASSOF')) "
                                              "RETURN COUNT(DISTINCT r) AS total"),
                                       report_name='all_relationships')
        if all_relationships is not None:
              self.non_isa_relationship_number = non_isa_relationships['total'][0]
              self.isa_relationship_number = isa_relationships['total'][0]
              self.all_relationship_number = all_relationships['total'][0]

        # nervous system relationships in ontology
        ns_non_isa_relationships = safe_gen_report(server=self.server,
                                              query=(
                                                  "MATCH (c:Nervous_system)-[r]->(d:Class) "
                                                  "WHERE c.short_form =~ 'FBbt.+' "
                                                  "AND (r.type = 'Related') RETURN COUNT(DISTINCT r) AS total"),
                                              report_name='non_isa_relationships')

        ns_isa_relationships = safe_gen_report(server=self.server,
                                          query=(
                                              "MATCH (c:Nervous_system)-[r]->(d:Class) WHERE c.short_form =~ 'FBbt.+' "
                                              "AND (type(r) = 'SUBCLASSOF') RETURN COUNT(DISTINCT r) AS total"),
                                          report_name='isa_relationships')

        ns_all_relationships = safe_gen_report(server=self.server,
                                          query=(
                                              "MATCH (c:Nervous_system)-[r]->(d:Class) WHERE c.short_form =~ 'FBbt.+' "
                                              "AND ((r.type = 'Related') OR (type(r) = 'SUBCLASSOF')) "
                                              "RETURN COUNT(DISTINCT r) AS total"),
                                          report_name='all_relationships')
        if ns_all_relationships is not None:
              self.ns_non_isa_relationship_number = ns_non_isa_relationships['total'][0]
              self.ns_isa_relationship_number = ns_isa_relationships['total'][0]
              self.ns_all_relationship_number = ns_all_relationships['total'][0]

        # images (excluding hemibrain 1.0.1)
        all_images = safe_gen_report(server=self.server,
                                query=("MATCH (i:Individual:has_image)-[]->(n:DataSet) "
                                       "WHERE n.production "
                                       "AND n.short_form<>\"Xu2020Neurons\" "
                                       "RETURN COUNT(DISTINCT i) AS images, "
                                       "COUNT(DISTINCT n) AS ds"),
                                report_name='all_images')

        single_neuron_images = safe_gen_report(server=self.server,
                                          query=("MATCH (n:DataSet)<-[]-"
                                                 "(i:Individual:Neuron:has_image)-"
                                                 "[:INSTANCEOF]->(c:Class:Neuron) "
                                                 "WHERE n.production "
                                                 "AND n.short_form<>\"Xu2020Neurons\" "
                                                 "RETURN COUNT(DISTINCT i) AS images, "
                                                 "COUNT(DISTINCT c) AS types"),
                                          report_name='single_neuron_images')

        exp_pattern_images = safe_gen_report(server=self.server,
                                        query=("MATCH (n:DataSet)<-[]-"
                                               "(i:Individual:Expression_pattern:has_image)-"
                                               "[:INSTANCEOF]->(c:Class:Expression_pattern) "
                                               "WHERE n.production "
                                               "RETURN COUNT(DISTINCT i) AS images, "
                                               "COUNT(DISTINCT c) AS drivers"),
                                        report_name='exp_pattern_images')

        split_images = safe_gen_report(server=self.server,
                                  query=("MATCH (n:DataSet)<-[]-(i:Split:has_image)-"
                                         "[:INSTANCEOF]->(c:Class:Split) "
                                         "WHERE n.production "
                                         "RETURN COUNT(DISTINCT i) AS images, "
                                         "COUNT(DISTINCT c) AS split_classes"),
                                  report_name='split_images')
                                  
        exp_pattern_fragment_images = safe_gen_report(server=self.server,
                                        query=("MATCH (n:DataSet)<-[]-"
                                               "(i:Individual:Expression_pattern_fragment:has_image)-"
                                               "[:part_of]->(c:Class:Expression_pattern) "
                                               "WHERE n.production "
                                               "RETURN COUNT(DISTINCT i) AS images, "
                                               "COUNT(DISTINCT c) AS drivers"),
                                        report_name='exp_pattern_fragment_images')
        if all_images is not None:
              self.all_image_number = all_images['images'][0]
              self.all_image_ds_number = all_images['ds'][0]
              self.single_neuron_image_number = single_neuron_images['images'][0]
              self.single_neuron_image_type_number = single_neuron_images['types'][0]
              self.exp_pattern_number = exp_pattern_images['images'][0]
              self.split_exp_pattern_driver_number = exp_pattern_images['drivers'][0]
              self.split_image_number = split_images['images'][0]
              self.split_image_driver_number = split_images['split_classes'][0]
              self.exp_pattern_fragment_number = exp_pattern_fragment_images['images'][0]
              self.split_exp_pattern_fragment_driver_number = exp_pattern_fragment_images['drivers'][0]

        # annotations

        driver_anatomy_annotations = safe_gen_report(server=self.server,
                                                query=("MATCH p=(ep:Class:Expression_pattern)<-"
                                                       "[r:part_of|overlaps]-(j:Individual)-[:INSTANCEOF]->(n:Class) "
                                                       "WHERE EXISTS(r.pub) AND n.short_form =~ 'FBbt.+' "
                                                       "RETURN COUNT(DISTINCT ep) AS EPs, COUNT(r) AS annotations, "
                                                       "COUNT(DISTINCT n) AS anatomy"),
                                                report_name='driver_anatomy_annotations')

        driver_ns_annotations = safe_gen_report(server=self.server,
                                                query=("MATCH p=(ep:Class:Expression_pattern)<-[r:part_of|overlaps]-"
                                                       "(j:Individual)-[:INSTANCEOF]->(n:Nervous_system:Class) "
                                                       "WHERE EXISTS(r.pub) AND n.short_form =~ 'FBbt.+' "
                                                       "RETURN COUNT(DISTINCT ep) AS EPs, COUNT(r) AS annotations, "
                                                       "COUNT(DISTINCT n) AS anatomy"),
                                                report_name='driver_ns_annotations')

        driver_neuron_annotations = safe_gen_report(server=self.server,
                                               query=("MATCH p=(ep:Class:Expression_pattern)<-[r:part_of]-"
                                                      "(j:Individual)-[:INSTANCEOF]->(n:Neuron:Class) "
                                                      "WHERE EXISTS(r.pub) AND n.short_form =~ 'FBbt.+' "
                                                      "RETURN COUNT(DISTINCT ep) AS EPs, COUNT(r) AS annotations, "
                                                      "COUNT(DISTINCT n) AS neurons"),
                                               report_name='driver_neuron_annotations')

        split_neuron_annotations = safe_gen_report(server=self.server,
                                              query=("MATCH p=(split:Class:Split)<-[r:part_of]-(j:Individual)-"
                                                     "[:INSTANCEOF]->(n:Neuron:Class) "
                                                     "WHERE EXISTS(r.pub) AND n.short_form =~ 'FBbt.+' "
                                                     "RETURN COUNT(DISTINCT split) AS Splits, COUNT(r) AS "
                                                     "annotations, COUNT(DISTINCT n) AS neurons"),
                                              report_name='split_neuron_annotations')
        if split_neuron_annotations is not None:
              self.driver_anatomy_annotations_EP_number = driver_anatomy_annotations['EPs'][0]
              self.driver_anatomy_annotations_annotation_number = driver_anatomy_annotations['annotations'][0]
              self.driver_anatomy_annotations_anatomy_number = driver_anatomy_annotations['anatomy'][0]
              self.driver_ns_annotations_EP_number = driver_ns_annotations['EPs'][0]
              self.driver_ns_annotations_annotation_number = driver_ns_annotations['annotations'][0]
              self.driver_ns_annotations_anatomy_number = driver_ns_annotations['anatomy'][0]
              self.driver_neuron_annotations_EP_number = driver_neuron_annotations['EPs'][0]
              self.driver_neuron_annotations_annotation_number = driver_neuron_annotations['annotations'][0]
              self.driver_neuron_annotations_neuron_number = driver_neuron_annotations['neurons'][0]
              self.split_neuron_annotations_split_number = split_neuron_annotations['Splits'][0]
              self.split_neuron_annotations_annotation_number = split_neuron_annotations['annotations'][0]
              self.split_neuron_annotations_neuron_number = split_neuron_annotations['neurons'][0]

        # connectivity

        neuron_connections = safe_gen_report(server=self.server,
                                        query=("MATCH (i:Individual:Neuron)-[r:synapsed_to]->"
                                               "(j:Individual:Neuron) "
                                               "WITH COLLECT(r) AS rels, COLLECT(DISTINCT i) AS ci, "
                                               "COLLECT(DISTINCT j) AS cj "
                                               "RETURN SIZE(apoc.coll.union(ci,cj)) AS neurons, "
                                               "SIZE(rels) AS connections"),
                                        report_name='synaptic_connections')
        if neuron_connections is not None:
              self.neuron_connections_neuron_number = neuron_connections['neurons'][0]
              self.neuron_connections_connection_number = neuron_connections['connections'][0]

        region_connections = safe_gen_report(server=self.server,
                                        query=("MATCH (n:Individual:Neuron)-"
                                               "[r:has_presynaptic_terminals_in|has_postsynaptic_terminal_in]"
                                               "->(m:Individual) "
                                               "RETURN COUNT(DISTINCT n) AS neurons, COUNT(DISTINCT m) AS regions, "
                                               "COUNT(DISTINCT r) AS connections"),
                                        report_name='synaptic_connections')

        self.region_connections_neuron_number = region_connections['neurons'][0]
        self.region_connections_region_number = region_connections['regions'][0]
        self.region_connections_connection_number = region_connections['connections'][0]

        muscle_connections = safe_gen_report(server=self.server,
                                        query=("MATCH (n:Neuron)-[r:synapsed_to|"
                                               "synapsed_via_type_Is_bouton_to|"
                                               "synapsed_via_type_Ib_bouton_to|"
                                               "synapsed_via_type_II_bouton_to|"
                                               "synapsed_via_type_III_bouton_to]->(m:Muscle) "
                                               "WITH n, r, m OPTIONAL MATCH (n2:Neuron)-[:SUBCLASSOF*]->(n) "
                                               "WITH COLLECT(DISTINCT r) AS rels, COLLECT(DISTINCT n) AS cn, "
                                               "COLLECT(DISTINCT n2) AS cn2, COLLECT(DISTINCT m) AS cm "
                                               "RETURN SIZE(apoc.coll.union(cn,cn2)) AS neurons, "
                                               "SIZE(cm) AS muscles, SIZE(rels) AS connections"),
                                        report_name='muscle_connections')
        if muscle_connections is not None:
              self.muscle_connections_neuron_number = muscle_connections['neurons'][0]
              self.muscle_connections_muscle_number = muscle_connections['muscles'][0]
              self.muscle_connections_connection_number = muscle_connections['connections'][0]

        sensory_connections = safe_gen_report(server=self.server,
                                         query=("MATCH (n:Neuron)-[r:has_sensory_dendrite_in]->(s:Sense_organ) "
                                                "WITH n, r, s OPTIONAL MATCH (n2:Neuron)-[:SUBCLASSOF*]->(n) "
                                                "WITH COLLECT(DISTINCT r) AS rels, COLLECT(DISTINCT n) AS cn, "
                                                "COLLECT(DISTINCT n2) AS cn2, COLLECT(DISTINCT s) AS cs "
                                                "RETURN SIZE(apoc.coll.union(cn,cn2)) AS neurons, "
                                                "SIZE(cs) AS sense_organs, SIZE(rels) AS connections"),
                                         report_name='sensory_connections')

        if sensory_connections is not None:
              self.sensory_connections_neuron_number = sensory_connections['neurons'][0]
              self.sensory_connections_sense_organ_number = sensory_connections['sense_organs'][0]
              self.sensory_connections_connection_number = sensory_connections['connections'][0]

        # Template data (excluding hemibrain v1.0.1)

        self.templates_data = safe_gen_report(server=self.server,
                                         query=("MATCH (d:DataSet)<-[:has_source]-(i:Individual)<-[:depicts]-"
                                                "(m:Individual)-[:in_register_with]->"
                                                "(:Template)-[:depicts]->(t:Template) "
                                                "WHERE d.short_form<>\"Xu2020Neurons\" "
                                                "OPTIONAL MATCH (m)-[:depicts]->(n:Individual:Neuron) "
                                                "OPTIONAL MATCH em = (m)-[:is_specified_output_of]->(e) "
                                                "WHERE e.label CONTAINS \"electron microscopy\" "
                                                "OPTIONAL MATCH (m)-[:depicts]->(ep:Individual:Expression_pattern) "
                                                "OPTIONAL MATCH (m)-[:depicts]->"
                                                "(epf:Individual:Expression_pattern_fragment) "
                                                "OPTIONAL MATCH (m)-[:depicts]->"
                                                "(s:Individual:Expression_pattern:Split) "
                                                "OPTIONAL MATCH pd = (m)-[:is_specified_output_of]->"
                                                "({label:\"computer graphic\"}) "
                                                "RETURN DISTINCT t.label AS template, COUNT(DISTINCT m) AS images, "
                                                "COUNT(DISTINCT d) AS datasets, "
                                                "COUNT(DISTINCT n) AS single_neuron_images, "
                                                "COUNT(DISTINCT em) AS em_images, "
                                                "COUNT(DISTINCT ep) AS expression_patterns, "
                                                "COUNT(DISTINCT s) AS split_images, "
                                                "COUNT(DISTINCT epf) AS expression_pattern_fragments, "
                                                "COUNT(DISTINCT pd) AS painted_domains"),
                                         report_name='templates_data')
        if self.templates_data is not None:
              self.templates_data.set_index('template', inplace=True, verify_integrity=True)


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
        f.new_line("(excludes hemibrain v1.0.1)", bold_italics_code='ic')
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
        f.new_line('**%s** images of expression pattern fragments of **%s** drivers'
                   % (str(self.exp_pattern_fragment_number),
                      str(self.split_exp_pattern_fragment_driver_number)))

        f.new_line()
        f.new_line("Annotations", bold_italics_code='bic')
        f.new_line()
        f.new_line('**%s** annotations recording **%s** types of anatomical structure that '
                   '**%s** specific driver lines are expressed in.'
                   % (str(self.driver_anatomy_annotations_annotation_number),
                      str(self.driver_anatomy_annotations_anatomy_number),
                      str(self.driver_anatomy_annotations_EP_number)))
        f.new_line('**%s** annotations recording **%s** parts of the nervous system that '
                   '**%s** specific driver lines are expressed in.'
                   % (str(self.driver_ns_annotations_annotation_number),
                      str(self.driver_ns_annotations_anatomy_number),
                      str(self.driver_ns_annotations_EP_number)))
        f.new_line('**%s** annotations recording **%s** types of neuron that '
                   '**%s** specific driver lines are expressed in.'
                   % (str(self.driver_neuron_annotations_annotation_number),
                      str(self.driver_neuron_annotations_neuron_number),
                      str(self.driver_neuron_annotations_EP_number)))
        f.new_line('**%s** annotations recording **%s** types of neuron that **%s** specific '
                   'split combinations are expressed in.'
                   % (str(self.split_neuron_annotations_annotation_number),
                      str(self.split_neuron_annotations_neuron_number),
                      str(self.split_neuron_annotations_split_number)))

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

        f.new_line()
        f.new_line("Content by Template", bold_italics_code='bic')
        f.new_line("(excludes hemibrain v1.0.1)", bold_italics_code='ic')

        f.new_line()
        template_table_content = ['Template Name', 'Datasets', 'Images', 'Single Neurons', 'EM Neurons',
                                  'Full Expression Patterns', 'Split Expression Patterns',
                                  'Partial Expression Patterns', 'Painted domains']
        for t in self.templates_data.index:
            template_table_content.extend([t, self.templates_data['datasets'][t], self.templates_data['images'][t],
                                           self.templates_data['single_neuron_images'][t],
                                           self.templates_data['em_images'][t],
                                           self.templates_data['expression_patterns'][t],
                                           self.templates_data['split_images'][t],
                                           self.templates_data['expression_pattern_fragments'][t],
                                           self.templates_data['painted_domains'][t]])

        f.new_table(columns=9, rows=(len(self.templates_data.index) + 1), text=template_table_content, text_align='left')
        f.new_line()

        f.create_md_file()


if __name__ == "__main__":
    # problems with connecting to 'pdb-alpha' and 'pdb-preview', consider adding later
    for s in ['pdb']:
        report = VFBContentReport(server=VFB_servers[s])
        report.get_info()
        report.prepare_report(filename=output_files[s])

