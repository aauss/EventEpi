from sklearn.base import TransformerMixin
import numpy as np


class MeanEmbeddingTransformer(TransformerMixin):

    def __init__(self):
        self._vocab, self._E = self._load_words()

    def _load_words(self):
        E = {}
        vocab = []

        with open('../nlp_surveillance/glove.6B.50d.txt', 'r', encoding="utf8") as file:
            for i, line in enumerate(file):
                l = line.split(' ')
                if l[0].isalpha():
                    v = [float(i) for i in l[1:]]
                    E[l[0]] = np.array(v)
                    vocab.append(l[0])
        return np.array(vocab), E

    def _get_word(self, v):
        for i, emb in enumerate(self._E):
            if np.array_equal(emb, v):
                return self._vocab[i]
        return None

    def _doc_mean(self, doc):
        return np.mean(np.array([self._E[w.lower().strip()] for w in doc if w.lower().strip() in self._E]), axis=0)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return np.array([self._doc_mean(doc) for doc in X])

    def fit_transform(self, X, y=None):
        return self.fit(X).transform(X)
