import re
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import FeatureUnion
from scipy.sparse import hstack
from utils import group_by, flatten

class LowerKeyDict(dict):
    def __getitem__(self, key):
        return self.store[key.lower()]

class KeywordExtractor():
    def __init__(self, keyword_array):
        # The CountVectorizer will pick up overlapping keywords, so for example
        # we will pickup "Foot and Mouth", "Hand, Foot and Mouth"
        # If we had a way to rule out matches like this we might see better perf.
        
        # We use a custom token pattern because the default requires word length
        # of 2 so it can't extract "Hepatitis A"
        # The http pattern is used to tokenize URLs.
        # It might be better to remove them during preprocessing.
        token_pattern = r'(?u)\b(?:(?:http\S+)|\w+)\b'
        case_sensitive_analyser = CountVectorizer(
            token_pattern=token_pattern,
            lowercase=False
        ).build_analyzer()
        case_insensitive_analyser = CountVectorizer(
            token_pattern=token_pattern,
        ).build_analyzer()
        case_sensitive = set()
        not_case_sensitive = set()
        for kw_obj in keyword_array:
            keyword = kw_obj['keyword']
            if kw_obj['case_sensitive']:
                case_sensitive.add(' '.join(case_sensitive_analyser(keyword)))
            else:
                not_case_sensitive.add(' '.join(case_insensitive_analyser(keyword)))
        self.case_sensitive_vectorizer = CountVectorizer(
            vocabulary=case_sensitive,
            ngram_range=(1, 1),
            token_pattern=token_pattern,
            lowercase=False
        )
        self.case_insensitive_vectorizer = CountVectorizer(
            vocabulary=not_case_sensitive,
            token_pattern=token_pattern,
            ngram_range=(1, 5)
        )

    def fit(self, X, y):
        pass
    def transform_with_vectorizer(self, vectorizer, texts):
        mat = vectorizer.fit_transform(texts)
        vocab = vectorizer.get_feature_names()
        out_dicts = []
        for r in range(mat.shape[0]):
            out_dict = {}
            for c in mat[r].nonzero()[1]:
                out_dict[vocab[c]] = mat[r,c]
            out_dicts.append(out_dict)
        return out_dicts
    def transform(self, texts):
        out_dicts = self.transform_with_vectorizer(
            self.case_insensitive_vectorizer,
            texts
        )
        out_dict_updates = self.transform_with_vectorizer(
            self.case_sensitive_vectorizer,
            texts
        )
        for d, upd in zip(out_dicts, out_dict_updates):
            d.update(upd)
        return out_dicts

class LinkedKeywordAdder():
    def __init__(self, keyword_array, weight=1):
        self.weight = weight
        self.keyword_links = LowerKeyDict({
            kw : set(flatten([item['linked_keywords'] for item in items], 1))
            for kw, items in group_by('keyword', keyword_array).items()
        })
    def fit(self, X, y):
        pass
    def transform_one(self, keyword_counts):
        out_dict = keyword_counts.copy()
        for k,v in keyword_counts.items():
            linked_keywords = self.keyword_links.get(k, [])
            for k2 in linked_keywords:
                out_dict[k2] = out_dict.get(k2, 0) + (float(v) * self.weight)
        return out_dict
    def transform(self, keyword_count_dicts):
        return map(self.transform_one, keyword_count_dicts)

class RenameAndMergeKeys():
    """
    Useful for reducing multiple synonyms in a word count dict into single keys.
    """
    def __init__(self, mappings):
        for k, v in mappings.items():
            resolved_destinations = []
            destination = v
            while destination in mappings:
                resolved_destinations += [destination]
                destination = mappings[destination]
                if destination in resolved_destinations:
                    destination = sorted(resolved_destinations)[0]
                    break
            mappings[k] = destination
        self.mappings = mappings
    def fit(self, X, y):
        pass
    def transform_one(self, keyword_counts):
        out_dict = keyword_counts.copy()
        for k,v in keyword_counts.items():
            k2 = self.mappings.get(k, k)
            out_dict[k2] = out_dict.get(k2, 0) + 1
        return out_dict
    def transform(self, keyword_count_dicts):
        return map(self.transform_one, keyword_count_dicts)

class LimitCounts():
    def __init__(self, max_count=1):
        self.max_count = max_count
    def fit(self, X, y):
        pass
    def transform_one(self, in_dict):
        return { k : min(self.max_count, v) for k,v in in_dict.items() }
    def transform(self, keyword_count_dicts):
        return map(self.transform_one, keyword_count_dicts)

class ProcessProperty():
    """
    Utility function for applying pipeline operations to dicts.
    E.g.:
    ProcessProperty(KeywordExtractor(), 'text', 'keyword_counts')
    transforms { "text" : "..." } to { "text" : "...", "keyword_counts" : {} }
    """
    def __init__(self, stage, inprop, outprop):
        self.stage = stage
        self.inprop = inprop
        self.outprop = outprop
    def fit(self, X_docs, y):
        X = [doc[self.inprop] for doc in X_docs]
        self.stage(X, y)
    def transform(self, X_docs):
        X = [doc[self.inprop] for doc in X_docs]
        for x in self.stage(X, y):
            X_docs[self.outprop] = x
        return X_docs
