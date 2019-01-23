from epitator.annotator import AnnoDoc
from epitator.geoname_annotator import GeonameAnnotator
from epitator.resolved_keyword_annotator import ResolvedKeywordAnnotator
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator


def annotate(text):
    if text is not None:
        anno_doc = AnnoDoc(text)
        anno_doc.add_tiers(GeonameAnnotator())
        anno_doc.add_tiers(ResolvedKeywordAnnotator())
        anno_doc.add_tiers(CountAnnotator())
        anno_doc.add_tiers(DateAnnotator())
        _delete_non_epitator_name_entity_tiers(anno_doc)
    else:
        anno_doc = None
    return anno_doc


def _delete_non_epitator_name_entity_tiers(anno_doc):
    del anno_doc.tiers["spacy.nes"]
    del anno_doc.tiers["spacy.noun_chunks"]
    del anno_doc.tiers["spacy.sentences"]
    del anno_doc.tiers["spacy.tokens"]
    del anno_doc.tiers["nes"]
    del anno_doc.tiers["ngrams"]
    del anno_doc.tiers["tokens"]


