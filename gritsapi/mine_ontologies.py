# coding: utf-8
"""
Mine keywords and their relationships from a set of ontologies so they can be
used by the classifier's feature extractor.
"""
import requests
import json
import re
import pickle
import nltk
from nltk import wordnet
synsets = wordnet.wordnet.synsets
import rdflib
import urlparse
import yaml, os
import urllib2
import sys


from diagnosis.utils import *
import ontology_file_helpers

# Specialized helpers
def traverse_hyponyms(synset, depth=3, only_nouns=False):
    all_hyponyms = []
    for syn in synset:
        d_r_lemmas = [syn]
        for lemma in syn.lemmas():
            d_r_lemmas.extend(lemma.derivationally_related_forms())
        lemmas = set([
            lemma.name().split('.')[0].replace('_', ' ')
            for lemma in d_r_lemmas
            if not(only_nouns and unicode(lemma).split('.')[1] != 'n')
        ])
        all_hyponyms.append({
            'synonyms' :  lemmas,
            'parent_synonyms' : set()
        })
        if depth > 0:
            child_hyponyms = traverse_hyponyms(syn.hyponyms(), depth-1)
            for hypo in child_hyponyms:
                hypo['parent_synonyms'] = hypo.get('parent_synonyms', set()) | lemmas
            all_hyponyms.extend(child_hyponyms)
    return all_hyponyms

def exclude_keywords(keyword_array, excluded):
    for k in keyword_array:
        k['synonyms'] -= excluded
        k['parent_synonyms'] -= excluded
    return keyword_array

def get_subject_to_label_dict(ontology):
    """
    Returns a dict that maps subjects in the ontology to label strings that
    could refer to them.
    """
    # Bidirectional symptom links could cause problems, so I'm avoiding them for now.
    qres = ontology.query("""
    SELECT ?subject ?label
    WHERE {
        { ?subject <http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym> ?label . } UNION
        { ?subject <http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym> ?label . } UNION
        { ?subject <http://www.geneontology.org/formats/oboInOwl#hasExactSynonym> ?label . } UNION
        { ?subject rdfs:label ?label . }
    }
    """)
    subject_to_labels = {}
    for subject, label in qres:
        subject = unicode(subject)
        label = unicode(label).strip()
        if subject not in subject_to_labels:
            subject_to_labels[subject] = set()
        subject_to_labels[subject].add(label)
    return subject_to_labels

def get_subject_to_parents_dict(ontology, root):
    qres = ontology.query("""
    SELECT ?subject ?parent
    WHERE {
        BIND (<%s> AS ?root)
        ?subject rdfs:subClassOf* ?root .
        ?subject rdfs:subClassOf ?parent .
    }
    """ % root)
    subject_to_parents = {}
    for subject, parent in qres:
        subject = unicode(subject)
        parent = unicode(parent)
        subject_to_parents[subject] = (
            subject_to_parents.get(subject, []) + [parent]
        )
    return subject_to_parents
    
def get_subject_to_ancestor_dict(subject_to_parents):
    @memoized
    def get_ancestors(subject):
        # The root's parent will not appear in subject_to_parents
        parents = subject_to_parents.get(subject, [])
        if subject in parents:
            parents.remove(subject)
        return set(parents + list(flatten(
            [get_ancestors(p) for p in parents], 1
        )))
    return { s : get_ancestors(s) for s in subject_to_parents.keys() }

def get_linked_keywords(ontology, root):
    """
    Get all the keywords (e.g. labels and synonyms) in an OBO ontology,
    and using the subclass predicate, link them to the labels/syns of
    ancestoral entities.
    """
    subject_to_labels = get_subject_to_label_dict(ontology)
    # Clean the labels
    for subject, labels in subject_to_labels.items():
        clean_labels = []
        for label in labels:
            # This regex removes some of the less useulf parenthetical notes 
            # from the disease ontology labels. Some labels include
            # parenthetical sections that we could parse into features. E.g.
            # (Thrombocytopenia: [primary] or [idopathic purpuric] or
            # [idiopathic] or [purpuric]) or (Evan's syndrome)
            # I'm passing those through as raw strings even though
            # they won't be useful keywords right now.
            c_label = re.sub(
                r"\s\((disorder|(morphologic abnormality)|finding)?\)",
                "", label
            )
            clean_labels.append(c_label)
        subject_to_labels[subject] = set(clean_labels)
    subject_to_parents = get_subject_to_parents_dict(ontology, root)
    #Filter out subjects without parents
    subject_to_labels = {
        k : subject_to_labels.get(k, set())
        for k in subject_to_parents.keys()
    }
    subject_to_ancestors = get_subject_to_ancestor_dict(subject_to_parents)
    # Remove the root because we don't have a label for it.
    for subject, ancestors in subject_to_ancestors.items():
        if root not in ancestors and subject != root:
            print subject, ancestors
            print subject_to_parents
            raise Exception("Root is not in ancestors")
        ancestors.remove(root)
    keywords = []
    for subject, labels in subject_to_labels.items():
        parent_synonyms = set(flatten(map(
            subject_to_labels.get,
            subject_to_ancestors[subject]
        ), 1))
        keywords.append({
            'parent_synonyms': parent_synonyms,
            'synonyms': labels
        })
        # TODO: Check for duplicate keywords this introduces
    return keywords

def download_google_sheet(sheet_url, default_type=None):
    """
    Utility for downloading EHA curated keywords from the given spreadsheet.
    """
    parsed_url = urlparse.urlparse(sheet_url)
    key = urlparse.parse_qs(parsed_url.query).get('key', [None])[0]
    if not key:
        prev = None
        for component in parsed_url.path.split('/'):
            if prev == 'd':
                key = component
                break
            else:
                prev = component
    request = requests.get(
        'https://spreadsheets.google.com/feeds/list/' + key +
        '/od6/public/values?alt=json-in-script&callback=jsonp'
    )
    spreadsheet_data = json.loads(
        request.text[request.text.find('jsonp(') + 6:-2]
    )
    keywords = []
    for entry in spreadsheet_data['feed']['entry']:
        kw_type = entry.get('gsx$type', {}).get('$t', default_type).strip()
        if len(kw_type) == 0:
            continue
        synonym_text = entry['gsx$synonyms'].get('$t')
        synonyms = [
            syn.strip() for syn in synonym_text.split(',')
        ] if synonym_text else []
        synonyms = filter(lambda k: len(k) > 0, synonyms)
        keyword = entry['gsx$keyword']['$t'].strip()
        keywords.append({
            'category': kw_type,
            'synonyms' : set([keyword] + synonyms),
        })
    return keywords

def healthmap_labels(other_synsets_to_add):
    curdir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(curdir, "healthmapLabels.yaml")) as f:
        hm_disease_labels = yaml.load(f)
    def preprocess_label(l):
        return re.sub('\W', '', l.lower())
        
    syn_obj_map = {}
    for syn_obj in other_synsets_to_add:
        for syn in syn_obj['synonyms']:
            key = preprocess_label(syn)
            syn_obj_map[key] = syn_obj_map.get(key, []) + [syn_obj]
    
    def detect_synonyms(disease):
        synonyms = [disease]
        for syn_obj in syn_obj_map.get(preprocess_label(disease), []):
            synonyms += syn_obj['synonyms']
        return set(synonyms)

    result = []
    for original_diease_label in hm_disease_labels:
        result.append({
            'hm_label' : original_diease_label,
            # Some labels such as Bronchitis/Bronchiolitis contain multiple synonyms.
            'synonyms' : set.union(
                *map(detect_synonyms, original_diease_label.split('/'))
            ),
            'category' : 'hm/disease'
        })
    return result

def wordnet_pathogens():
    # WordNet : Pathogens
    synset = (
        synsets("pathogen")[0].hypernyms() +
        synsets("virus")
    )
    all_wn_pathogens = traverse_hyponyms(synset)
    pathogens = exclude_keywords(
        all_wn_pathogens,
        set(['computer virus'])
    )
    for p in pathogens:
        for s in p['synonyms']:
            if len(p) < 2: p['synonyms'] -= set([p])
        for s in p['parent_synonyms']:
            if len(p) < 2: p['parent_synonyms'] -= set([p])
    print len(pathogens), "wordnet pathogens found"
    return pathogens

def wordnet_hostnames():
    # WordNet : Host names
    # A lot of these are pretty farfetched.
    # It would be a good idea to add some other sources for hosts.
    synset = (
        synsets("insect")[:1] +
        synsets("animal")[:1] +
        synsets("mammal") +
        synsets('plant')[1:2]
    )
    #Definately an incomplete list:
    non_host_names = set(['sponge', 'pest', 'mate',
                          'big game', 'prey',
                          'young', 'work', 'worker', 'head',
                          'dobson', # More likely a last name
                          'carnivore',
                          'dam',
                          'giant', 'world',
                          'medusa',
                          'simple',
                          'Neandertal',
                          'annual',
                          'tom',
                          'A', 'E', 'C'])
    probably_not_host_names = set([
        'soldier',
        'pooch', 'kitten',
        'game',
        # We might miss disease carrying hawthorns because of this
        # however the false positive rate will cause bigger problems.
        'may',
        'mastodon',
        'lapdog', 'young bird',
        'young fish'
    ])
    hostnames = exclude_keywords(
        traverse_hyponyms(synset, only_nouns=True),
        non_host_names | probably_not_host_names
    )
    print len(hostnames), "wordnet hostnames found"
    return hostnames
                      
def all_wordnet_keywords():
    keywords = []
    for s in wordnet_pathogens():
        s['category'] = 'wordnet/pathogens'
        keywords.append(s)
    for s in wordnet_hostnames():
        s['category'] = 'wordnet/hosts'
        keywords.append(s)
    for s in traverse_hyponyms([
        synsets("season")[1]] + 
        synsets("annual")
    ):
        s['category'] = 'wordnet/season'
        keywords.append(s)
    for s in traverse_hyponyms(synsets("weather")[:1]):
        s['category'] = 'wordnet/climate'
        keywords.append(s)
    for s in traverse_hyponyms(synsets("massive") + synsets("large")):
        s['category'] = 'wordnet/mod/large'
        keywords.append(s)
    for s in traverse_hyponyms(synsets("dangerous")):
        s['category'] = 'wordnet/mod/severe'
        keywords.append(s)
    for s in traverse_hyponyms(synsets("atypical") + synsets("uncommon")):
        s['category'] = 'wordnet/mod/rare'
        keywords.append(s)
    for s in traverse_hyponyms(synsets("painful")):
        s['category'] = 'wordnet/mod/painful'
        keywords.append(s)
    return keywords

# [The Disease ontology](http://disease-ontology.org/downloads/)
def mine_disease_ontology():
    # The subclass relationships in the disease ontology could help us
    # as additional general category labels to the Health Map data for things
    # like "primary bacterial infectious disease".
    # Every disease is a class which seems strange to me,
    # I think it is more natural to think of them as instances of classes.
    # Some subjects that appear to be diseases have no subClassOf predicate.
    # They do however have inSubset predicates.
    # The ones I spot checked have depricated predicates set to true,
    # so I think we can ignore them.
    # A few subjects have multiple subClassOf predicates.
    disease_ontology = rdflib.Graph()
    disease_ontology.parse(
        "http://purl.obolibrary.org/obo/doid.owl",
        format="xml"
    )
    # Many diseases have predicates like has_symptom listed as plain text in
    # their definition, this code extracts them.
    def create_re_for_predicate(predicate):
        esc_pred = re.escape(predicate)
        return re.compile(
            "(" + predicate + 
            r"\s{1,2}(the\s{1,2})?(?P<" + predicate + ">(.+?))" +
            r")" +
            r"(,|\.|(\s(or|and|\w+\_\w+)\b))",
            re.I
        )
    
    predicates = [
        "has_symptom",
        "transmitted_by",
        "has_material_basis_in",
        "results_in",
        "located_in"
    ]
    
    doid_res = map(create_re_for_predicate, predicates)
    
    def parse_doid_def(def_str):
        for doid_re in doid_res:
            for m in doid_re.finditer(def_str):
                yield m.groupdict()

            
    qres = disease_ontology.query("""
    SELECT DISTINCT ?def
    WHERE {
        BIND (<http://purl.obolibrary.org/obo/IAO_0000115> AS ?defined_as)
        BIND (<http://purl.obolibrary.org/obo/DOID_4> AS ?disease)
        ?subject rdfs:subClassOf* ?disease .
        ?subject ?defined_as ?def .
    }
    """)
    
    disease_predicates = flatten([
        list(parse_doid_def(unicode(r)))
        for r in qres
    ], 1)
    
    doid_keywords = []

    for disease_predicate in disease_predicates:
        for predicate, value in disease_predicate.items():
            doid_keywords.append({
                'synonyms': [value.strip()],
                'category': 'doid/' + predicate,
            })
    for keyword_object in get_linked_keywords(
        disease_ontology,
        "http://purl.obolibrary.org/obo/DOID_4"
    ):
        new_synonyms = set()
        for synonym in keyword_object['synonyms']:
            synonym = unicode(synonym).strip()
            misc_paren_notes = [
                "context-dependent category",
                "qualifier value",
                "clinical",
                "acute",
                "body structure",
                "category"
            ]
            m = re.match(
                "(?P<label>.+?)\s+\((?P<misc_note>" +
                '|'.join(map(re.escape, misc_paren_notes)) +
                ")\)",
                synonym, re.I
            )
            if m:
                d = m.groupdict()
                synonym = d['label']
            new_synonyms.add(synonym.strip())
        keyword_object['category'] = 'doid/diseases'
        doid_keywords.append(keyword_object)
    print len(doid_keywords), "keywords extracted from doid"
    return doid_keywords

# [Symptom Ontology](http://purl.obolibrary.org/obo/ido.owl)
def mine_symptom_ontology():
    symptom_ontology = rdflib.Graph()
    symptom_ontology.parse(
        "http://purl.obolibrary.org/obo/symp.owl",
        format="xml"
    )
    symp_keywords = []
    for keyword_object in get_linked_keywords(
        symptom_ontology,
        "http://purl.obolibrary.org/obo/SYMP_0000462"
    ):
        keyword_object['category'] = 'doid/symptoms'
        symp_keywords.append(keyword_object)
    print "Symptoms in the symptom ontology:", len(symp_keywords)
    return symp_keywords

# [The Infectious Disease Ontology](http://www.ontobee.org/browser/index.php?o=IDO)
# I haven't found much of use in this ontology.
# It seems more oriented toward higher level reasoning than providing taxonomies and synsets.
# ido = rdflib.Graph()
# ido.parse("http://purl.obolibrary.org/obo/ido.owl", format="xml")

def biocaster_keywords_with_subject(g, subject_condition):
    """
    Query the biocaster ontology using the given subject condition
    and return syn sets for all the terminology found.
    """
    results = list(g.query("""
    SELECT DISTINCT ?instance ?label ?synonym
    WHERE {
        """ + subject_condition + """ .
        ?instance a ?subject .
        OPTIONAL { ?instance biocaster:label ?label } .
        ?instance biocaster:synonymTerm ?synonym
    }
    """, initNs={
        'biocaster': rdflib.URIRef("http://biocaster.nii.ac.jp/biocaster#"),
        'rdfsn': rdflib.URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    }))
    instances = {
        instance_ref : {
            'label' : unicode(label),
            'synonyms' : set([unicode(label)] if label else []),
        }
        for instance_ref, label, noop in results
    }
    synonyms_to_skip = [
        'Consumption', # It rarely means TB, so it is better not to use it.
    ]
    for instance_ref, noop, syn_ref in results:
        if syn_ref in g.synonym_to_label:
            syn_label = g.synonym_to_label[syn_ref]
            if syn_label not in synonyms_to_skip:
                instances[instance_ref]['synonyms'] |= set([syn_label])
        else:
            #Non english term
            pass
    for instance in instances.values():
        new_synonyms = set()
        for synonym in instance['synonyms']:
            synonym = unicode(synonym).strip()
            hosts = [
                "Avian",
                "Bovine",
                "Caprine",
                "Canine",
                "Cervine",
                "Feline",
                "Swine",
                "Non-Human Primate",
                # I found an entry with a typo: Adenovirus infection (Humasn)
                # TODO: We should fix the data or add an alias.
                "Human",
                "Honeybee",
                "Fish",
                "Rodent",
                "Equine",
                "Lagomorph"
            ]
            m = re.match(
                "(?P<label>.+?)\s+\((?P<host>" +
                '|'.join(map(re.escape, hosts)) +
                ")\)",
                synonym, re.I
            )
            if m:
                d = m.groupdict()
                synonym = d['label']
                instance['host'] = d['host']
            new_synonyms.add(synonym.strip())
        instance['synonyms'] = new_synonyms
    return instances

# [Biocaster ontology](https://code.google.com/p/biocaster-ontology/downloads/detail?name=BioCaster2010-30-Aug-904.owl&can=2&q=)
def mine_biocaster_ontology():
    # The ontology is composed of subject-relationship-object triples.
    # For example: `("Python", "is a", "programming language")`
    # I think one of the particularly interesting things about biocaster is that
    # they are using these relationships for diagnosis.
    # For example:
    # 
    # http://biocaster.nii.ac.jp/biocaster#FeverSymptomHuman_4447
    # http://biocaster.nii.ac.jp/biocaster#indicates
    # http://biocaster.nii.ac.jp/biocaster#DISEASE_491

    g = rdflib.Graph()
    g.parse(
        "https://biocaster-ontology.googlecode.com/files/BioCaster2010-30-Aug-904.owl",
        format="xml"
    )
    qres = g.query("""
    SELECT DISTINCT ?synonym ?label
    WHERE {
        ?synonym rdfsn:type biocaster:englishTerm .
        ?synonym biocaster:label ?label
    }
    """, initNs={
        'biocaster': rdflib.URIRef("http://biocaster.nii.ac.jp/biocaster#"),
        'rdfsn': rdflib.URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    })
    g.synonym_to_label = {
        syn_ref : unicode(label)
        for syn_ref, label  in qres    
    }
    biocaster_symptom_syns = map(
        lambda d: dict(d, category='biocaster/symptoms'),
        biocaster_keywords_with_subject(g,
            "?subject rdfs:subClassOf* biocaster:SYMPTOM"
        ).values()
    )

    biocaster_disease_syns = map(
        lambda d: dict(d, category='biocaster/diseases'),
        biocaster_keywords_with_subject(g,
            "?subject rdfs:subClassOf* biocaster:DISEASE"
        ).values()
    )
    
    biocaster_pathogen_syns = map(
        lambda d: dict(d, category='biocaster/pathogens'),
        biocaster_keywords_with_subject(g,
            """
            { ?subject rdfs:subClassOf* biocaster:BACTERIUM } UNION
            { ?subject rdfs:subClassOf* biocaster:VIRUS } UNION
            { ?subject rdfs:subClassOf* biocaster:FUNGUS } UNION
            { ?subject rdfs:subClassOf* biocaster:PROTOZOAN }
            """
        ).values()
    )
    
    return (
        biocaster_symptom_syns +
        biocaster_disease_syns +
        biocaster_pathogen_syns
    )

def mine_usgs_ontology():
    # [Terrain](http://cegis.usgs.gov/ontology.html#constructing_ontologies)
    # This keyword set is used to capure environmental factors. For example,
    # a disease might be related to swamps or cities with high populations density.
    terrain_ontology = rdflib.Graph()

    files = [   "http://cegis.usgs.gov/docs/Event.ttl", 
            "http://cegis.usgs.gov/docs/Division.ttl", 
            "http://cegis.usgs.gov/docs/BuiltUpArea.ttl", 
            "http://cegis.usgs.gov/docs/EcologicalRegime.ttl",
            "http://cegis.usgs.gov/docs/SurfaceWater.ttl",
            "http://cegis.usgs.gov/docs/Terrain.ttl"]

    for ttlFile in files:
        try:
            terrain_ontology.parse( 
                ttlFile,
                format="n3"
            )
        except urllib2.HTTPError:
            print "*********Problem downloading terrain_ontology file:", ttlFile, "*********" 

    usgs_keywords = []
    for keyword_object in get_linked_keywords(
        terrain_ontology,
        "http://www.w3.org/2002/07/owl#Thing"
    ):
        keyword_object['category'] = 'usgs/terrain'
        usgs_keywords.append(keyword_object)
    return usgs_keywords

def eha_keywords():
    keywords = []
    keywords.extend(download_google_sheet(
        'https://docs.google.com/a/ecohealth.io/spreadsheets/d/1M4dIaV7_YanJdau2sJuRt3LmF71h2q_Wf93qFSzMdoY/edit#gid=0',
    ))
    keywords.extend(download_google_sheet(
        'https://docs.google.com/a/ecohealth.io/spreadsheet/ccc?key=0AuwHL_SlxPmAdDRyYnRDNzFRbnlOSHM2NlZtVFNRVGc#gid=0',
        default_type='disease'
    ))
    keywords.extend(download_google_sheet(
        'https://docs.google.com/a/ecohealth.io/spreadsheet/ccc?key=0AuwHL_SlxPmAdEFQUUxMUjRnVDZvQUR6UFZFdC1FelE#gid=0',
        default_type='symptom'
    ))
    for keyword in keywords:
        keyword['category'] = 'eha/' + keyword['category']
    return keywords

def create_keyword_object_array(synset_object_array):
    blocklist = set([
        'can', 'don', 'dish', 'ad', 'mass', 'yellow', 'the', 'Consumption',
    ])
    keyword_object_array = []
    keywords_sofar = {}
    for synset_object in synset_object_array:
        for kw in synset_object['synonyms']:
            if kw in blocklist:
                raise Exception("Blocked keyword: " + kw + ' in ' + unicode(synset_object))
            if kw.strip() != kw:
                raise Exception("Untrimmed keyword: " + kw + ' in ' + unicode(synset_object))
            if len(kw) == 0:
                print "Empty keyword in", synset_object
                continue
            # commenting this out to cleanup script output
            # if ')' == kw[-1]:
            #     print "Parenthetical keyword:", kw, 'in', unicode(synset_object)

            keywords_sofar[kw] = synset_object
            keyword_object_array.append({
                'keyword' : kw,
                'category' : synset_object['category'],
                'linked_keywords' : [
                    '[linked] ' + lkw
                    for lkw in set(synset_object.get('parent_synonyms', [])) # | set([kw]) # not sure whether to include the kw
                ],
                'synset_object' : synset_object,
                'case_sensitive' : (
                    ' ' not in kw and
                    len(kw) <= 6 and
                    kw.upper() == kw
                ),
                'duplicate' : kw in keywords_sofar
            })
    print "Total keywords:", len(keyword_object_array)
    return keyword_object_array

def compare_last_two_pickles():
    new, previous = ontology_file_helpers.get_ontologies_to_compare()
    previousPickle = set([k['keyword'].lower() for k in pickle.load(open(previous))])
    currentPickle = set([k['keyword'].lower() for k in pickle.load(open(new))])
    print "New keywords:", len(currentPickle - previousPickle)
    print "Removed keywords:", len(previousPickle - currentPickle)

if __name__ == "__main__":
    print "gathering keywords..."
    disease_kws = mine_disease_ontology() +\
        mine_biocaster_ontology() +\
        eha_keywords()
    keywords = create_keyword_object_array(
        all_wordnet_keywords() +
        disease_kws +
        mine_symptom_ontology() +
        mine_usgs_ontology() +
        healthmap_labels(disease_kws)
    )
    print "creating pickle..."
    print """
    To update the ontology data we use in our deployments use this command:
    aws s3 cp ontologies-x.x.x.p s3://classifier-data/ --region us-west-1
    """
    newPickle = ontology_file_helpers.get_next_ontology_file_name();
    with open(newPickle, 'wb') as f:
        pickle.dump(keywords, f)
    print "new pickle created.  Comparing to previous version..."
    compare_last_two_pickles()
