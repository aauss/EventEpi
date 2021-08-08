from collections import Counter
from multiprocessing import Pool, cpu_count
from typing import Dict, List, Optional, Union

from epitator.annotator import AnnoDoc
from epitator.geoname_annotator import GeonameAnnotator
from epitator.resolved_keyword_annotator import ResolvedKeywordAnnotator


class Summarizer:
    def __init__(self):
        self.doc: Optional[AnnoDoc] = None

    def summarize(self, text: Union[str, List[str]]) -> Dict[str, str]:
        if not isinstance(text, str):
            return self._batch_summarize(text)
        else:
            self.doc = AnnoDoc(text)
            self.doc.add_tiers(ResolvedKeywordAnnotator())
            self.doc.add_tiers(GeonameAnnotator())
            return {
                "disease": self._extract_key_disease(),
                "geoname": self._extract_key_geoname(),
            }

    def _batch_summarize(self, texts) -> List[Dict[str, str]]:
        with Pool(cpu_count() - 1) as p:
            return p.map(self.summarize, texts)

    def _extract_key_disease(self):
        try:
            diseases = [
                i.metadata["resolutions"][0]["entity"]["label"]
                for i in self.doc.tiers["resolved_keywords"].spans
            ]
            return Counter(diseases).most_common(1)[0][0]
        except (KeyError, IndexError):
            return ""

    def _extract_key_geoname(self):
        try:
            geonames = [i.geoname for i in self.doc.tiers["geonames"].spans]
            country_of_geoname = []
            for geoname in geonames[:3]:
                try:
                    country_of_geoname.append(geoname.country_name)
                except AttributeError:
                    country_of_geoname.append(geoname.name)
            return Counter(country_of_geoname).most_common(1)[0][0]
        except (KeyError, IndexError):
            return ""
