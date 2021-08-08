import pickle
import re
import sys
import time
import unicodedata
import warnings
from collections import namedtuple
from dataclasses import dataclass
from functools import partial
from itertools import product
from pathlib import Path
from typing import ClassVar, Dict, List, Optional, Tuple, Union

warnings.filterwarnings("ignore")

import matplotlib.pyplot as plt
import nltk
import numpy as np
import pandas as pd
from epitator.annotator import AnnoDoc
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator
from gensim.models import KeyedVectors
from imblearn.metrics import (
    classification_report_imbalanced,
    geometric_mean_score,
    make_index_balanced_accuracy,
)
from imblearn.over_sampling import ADASYN
from nltk import pos_tag, sent_tokenize, word_tokenize
from nltk.corpus import stopwords, wordnet
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import PunktSentenceTokenizer
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, make_scorer
from sklearn.model_selection import GridSearchCV, learning_curve, train_test_split
from sklearn.naive_bayes import BernoulliNB, ComplementNB, MultinomialNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import Pipeline
from sklearn.svm import SVC
from tqdm import tqdm

sys.path.append(str((Path(__file__).parent.resolve() / Path("..")).resolve()))
from eventepi.corpus_reader import PickledCorpusReader
from eventepi.idb import IDB


@dataclass
class DataLoader:
    df = IDB().preprocess().df_processed
    corpus = PickledCorpusReader()

    def labeled_texts(self) -> Tuple[Dict[str, str], List[bool]]:
        """Labels fileids into being relevant or not relevant.

        Returns:
            List of file IDs and their labels whether they are relevant True or not relevant False.
        """
        ids_of_2018 = []
        for id_ in self.corpus.fileids():
            try:
                if re.search(r"\D(\d{4})\D", id_)[1] == "2018":
                    ids_of_2018.append(id_)
            except TypeError:
                continue
        labels = [
            True if id_.replace(".pickle", ".html") in list(self.df.fileid) else False
            for id_ in ids_of_2018
        ]
        texts = list(self.corpus.docs(ids_of_2018))
        return texts, labels

    def labeled_count_sentences(self) -> Tuple[List[str], List[bool]]:
        texts, targets = self._get_texts_with_targets("case_counts_idb")

        all_sentences = np.array([])
        all_labels = np.array([])
        for text, target in tqdm(
            list(zip(texts, targets)), desc="Create labeled sentences"
        ):
            sentences, entities = self._get_sentences_with_entities(text, "counts")
            labels = [target == count for count in entities]
            all_sentences = np.append(all_sentences, sentences)
            all_labels = np.append(all_labels, labels)
        not_na = ~np.isnan(all_labels.astype(float))
        return all_sentences[not_na], all_labels[not_na].astype(int)

    def labeled_date_sentences(self) -> Tuple[List[str], List[bool]]:
        texts, targets = self._get_texts_with_targets("date_cases_idb")

        all_sentences = np.array([])
        all_labels = np.array([], dtype=int)
        for text, target in tqdm(
            list(zip(texts, targets)), desc="Create labeled sentences"
        ):
            sentences, entities = self._get_sentences_with_entities(text, "dates")
            labels = self._label_dates(entities, pd.to_datetime(target))
            all_sentences = np.append(all_sentences, sentences)
            all_labels = np.append(all_labels, labels)
        not_na = ~np.isnan(all_labels.astype(float))
        return all_sentences[not_na], all_labels[not_na].astype(int)

    def _get_texts_with_targets(
        self, entity: str
    ) -> Tuple[List[str], List[Union[float, str]]]:
        df_with_targets = self.df[self.df[entity].notna()]
        texts = self.corpus.docs(
            df_with_targets["fileid"].str.replace(".html", ".pickle")
        )
        target = df_with_targets[entity].values
        return texts, target

    def _label_dates(
        self, entities: List[List["datetime.datetime"]], target: pd.Timestamp
    ) -> List[Optional[int]]:
        Date = namedtuple("DateRange", ["from_", "to_", "target"])
        dates = [
            Date(
                pd.to_datetime(from_, errors="coerce"),
                pd.to_datetime(to_, errors="coerce"),
                target,
            )
            for from_, to_ in entities
        ]
        filtered_dates = [
            date if date.to_ - date.from_ <= pd.Timedelta("7days") else None
            for date in dates
        ]
        return [
            int(self._is_in_date_range(date, "3days")) if date else None
            for date in filtered_dates
        ]

    def _is_in_date_range(self, date: pd.Timestamp, allowed_margin: str) -> bool:
        return ((date.from_ - pd.Timedelta(allowed_margin)) <= date.target) & (
            (date.to_ + pd.Timedelta(allowed_margin)) >= date.target
        )

    def _get_sentences_with_entities(
        self, text: str, entity: str
    ) -> Tuple[List[str], List[Union["datetime.datetime", int]]]:
        annotated = self._annotate(text, entity)
        span_entity_dict = self._create_span_entity_dict(annotated, entity)
        return self._filter_sentences_with_entities(annotated, span_entity_dict)

    def _annotate(self, text: str, entity: str) -> AnnoDoc:
        tier = {"counts": CountAnnotator(), "dates": DateAnnotator()}
        annotated = AnnoDoc(text)
        annotated.add_tiers(tier[entity])
        return annotated

    def _create_span_entity_dict(
        self, annotated: AnnoDoc, entity: str
    ) -> Dict[Tuple[int, int], Union["datetime.datetime", int]]:
        spans = annotated.tiers[entity].spans
        to_metadata_attr = {"counts": "count", "dates": "datetime_range"}
        metadata_attr = to_metadata_attr[entity]
        return {(span.start, span.end): span.metadata[metadata_attr] for span in spans}

    def _filter_sentences_with_entities(
        self,
        annotated: AnnoDoc,
        span_entity_dict: Dict[Tuple[int, int], Union["datetime.datetime", int]],
    ) -> Tuple[List[str], List[Union["datetime.datetime", int]]]:
        sentence_spans = PunktSentenceTokenizer().span_tokenize(annotated.text)
        entity_spans = span_entity_dict.keys()
        cartesian_product = product(entity_spans, sentence_spans)
        entity_sentence_spans = list(filter(self._overlaps, cartesian_product))

        entities = [span_entity_dict[span[0]] for span in entity_sentence_spans]
        sentences = [annotated.text[slice(*span[1])] for span in entity_sentence_spans]
        return sentences, entities

    def _overlaps(
        self, tuple_of_tuples: Tuple[Tuple[int, int], Tuple[int, int]]
    ) -> Optional[bool]:
        entity_span, sent_span = tuple_of_tuples
        if sent_span[0] <= entity_span[0] and entity_span[1] <= sent_span[1]:
            return True


@dataclass
class TextNormalizer(BaseEstimator, TransformerMixin):

    stopwords = set(stopwords.words("english"))
    lemmatizer = WordNetLemmatizer()

    def is_punct(self, token):
        return all(unicodedata.category(char).startswith("P") for char in token)

    def is_stopword(self, token):
        return token.lower() in self.stopwords

    def normalize(self, document):
        return [
            self.lemmatize(token, tag).lower()
            for sentence in sent_tokenize(document)
            for (token, tag) in pos_tag(word_tokenize(sentence))
            if not self.is_punct(token) and not self.is_stopword(token)
        ]

    def lemmatize(self, token, pos_tag):
        tag = {
            "N": wordnet.NOUN,
            "V": wordnet.VERB,
            "R": wordnet.ADV,
            "J": wordnet.ADJ,
        }.get(pos_tag[0], wordnet.NOUN)

        return self.lemmatizer.lemmatize(token, tag)

    def fit(self, X, y=None):
        return self

    def transform(self, documents):
        for document in documents:
            yield self.normalize(document)


@dataclass
class EmbeddingNormalizer:

    stopwords = set(nltk.corpus.stopwords.words("english"))
    tag_remover = re.compile(r"<.*?>")

    def is_punct(self, token):
        return all(unicodedata.category(char).startswith("P") for char in token)

    def is_stopword(self, token):
        return token.lower() in self.stopwords

    def normalize(self, document):
        return [
            token.lower()
            for sentence in sent_tokenize(document)
            for token in word_tokenize(sentence)
            if not self.is_punct(token) and not self.is_stopword(token)
        ]

    def read_file(self, path):
        if "pickle" in path:
            with open(path, "rb") as f:
                return pickle.load(f)
        else:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()


@dataclass
class MeanDocumentEmbedder(BaseEstimator, TransformerMixin):
    word2vec = KeyedVectors.load(
        str(Path(__file__).parent.resolve() / Path("../data/embeddings/embeddings_300"))
    ).wv
    normalizer = EmbeddingNormalizer()

    def mean_document_embed(self, text):
        normalized = self.normalizer.normalize(text)
        return np.mean(
            [self.word2vec[word] for word in normalized if word in self.word2vec],
            axis=0,
        )

    def fit(self, X, y=None):
        return self

    def transform(self, documents):
        for document in documents:
            yield self.mean_document_embed(document)


@dataclass
class Trainer:
    loader = DataLoader()
    iba = make_index_balanced_accuracy()(geometric_mean_score)
    train_test_split = partial(train_test_split, test_size=0.25, random_state=13353)
    _file_path = Path(__file__).parent.resolve()

    def train_relevance_scoring(self):
        X, y = self.loader.labeled_texts()
        X_train, X_test, y_train, y_test = self.train_test_split(X, y)

        # BOW models
        grid_search_parameters = {
            "tfidf__ngram_range": [(1, 1), (1, 3)],
            "tfidf__use_idf": (True, False),
        }

        for model_name, model in [
            ("complement", ComplementNB),
            ("multinomial", MultinomialNB),
        ]:

            pipeline = Pipeline(
                [
                    ("norm", TextNormalizer()),
                    (
                        "tfidf",
                        TfidfVectorizer(
                            tokenizer=self._identity, preprocessor=None, lowercase=False
                        ),
                    ),
                    ("clf", model(alpha=0.001)),
                ]
            )

            gs_model = GridSearchCV(
                pipeline,
                grid_search_parameters,
                scoring=make_scorer(
                    make_index_balanced_accuracy()(geometric_mean_score)
                ),
                verbose=2,
            )

            start_time = time.time()
            gs_model = gs_model.fit(X_train, y_train)
            training_time = f"{int(time.time()-start_time)/60:.1f}"
            best_params = gs_model.best_params_

            predicted = gs_model.predict(X_test)
            report = classification_report_imbalanced(y_test, predicted)

            with open(
                self._file_path
                / Path(f"../data/results/relevance_{model_name}_report.txt"),
                "w",
            ) as f:
                f.write(
                    str(best_params)
                    + "\n\n"
                    + report
                    + "\n\n"
                    + f"training time: {training_time} min."
                )

            self.plot_confusion_matrix(
                confusion_matrix(y_test, predicted),
                ["irrelevant", "relevant"],
                f"Confusion matrix, relevance scoring, {model_name} NBC",
            )

            self.plot_learning_curve(
                pipeline.set_params(**best_params),
                f"Learning curve, relevance scoring, {model_name} NBC",
                X,
                y,
            )

            with open(
                self._file_path
                / Path(f"../data/results/relevance_{model_name}_model.pickle"),
                "wb",
            ) as f:
                pickle.dump(gs_model.best_estimator_, f)

        # Embedding models without ADASYN
        embedder = MeanDocumentEmbedder()

        X_embedded = np.array(list(embedder.transform(X)))
        (X_train_embedded, X_test_embedded, y_train, y_test) = self.train_test_split(
            X_embedded, y
        )

        for model_name, model in [
            ("logistic regression", LogisticRegression()),
            ("k-nearest neighbors", KNeighborsClassifier()),
            ("support vector classifier", SVC()),
            ("multi layer perceptron", MLPClassifier()),
        ]:
            start_time = time.time()
            model.fit(X_train_embedded, y_train)
            training_time = f"{int(time.time()-start_time)/60:.1f}"
            predicted = model.predict(X_test_embedded)
            report = classification_report_imbalanced(y_test, predicted)

            with open(
                self._file_path
                / Path(f"../data/results/relevance_no_adasyn_{model_name}_report.txt"),
                "w",
            ) as f:
                f.write(report + "\n\n" + f"training time: {training_time} min.")
            self.plot_confusion_matrix(
                confusion_matrix(y_test, predicted),
                ["irrelevant", "relevant"],
                f"Confusion matrix (no ADASYN), relevance scoring, {model_name}",
            )

        # Embedding models with ADASYN
        adasyn = ADASYN(random_state=13353)
        X_resample, y_resample = adasyn.fit_sample(X_train_embedded, y_train)
        for model_name, model in [
            ("logistic regression", LogisticRegression),
            ("k-nearest neighbors", KNeighborsClassifier),
            ("support vector classifier", SVC),
            ("multi layer perceptron", MLPClassifier),
        ]:
            if model_name == "support vector classifier":
                clf = model(probability=True)
            else:
                clf = model()
            start_time = time.time()
            clf = clf.fit(X_resample, y_resample)
            training_time = f"{int(time.time()-start_time)/60:.1f}"
            predicted = clf.predict(X_test_embedded)
            report = classification_report_imbalanced(y_test, predicted)

            with open(
                self._file_path
                / Path(f"../data/results/relevance_{model_name}_report.txt"),
                "w",
            ) as f:
                f.write(report + "\n\n" + f"training time: {training_time} min.")

            with open(
                self._file_path
                / Path(f"../data/results/relevance_{model_name}_model.pickle"),
                "wb",
            ) as f:
                pickle.dump(clf, f)

            self.plot_confusion_matrix(
                confusion_matrix(y_test, predicted),
                ["irrelevant", "relevant"],
                f"Confusion matrix, relevance scoring, {model_name}",
            )

            self.plot_learning_curve(
                model(),
                f"Learning curve, relevance scoring, {model_name}",
                X_resample,
                y_resample,
            )

    def train_key_entity_classifications(self):
        X_date, y_date = self.loader.labeled_date_sentences()
        self._train_key_entity_classification(X_date, y_date, "date")

        X_count, y_count = self.loader.labeled_count_sentences()
        self._train_key_entity_classification(X_count, y_count, "count")

    def _train_key_entity_classification(self, X, y, entity):
        X_train, X_test, y_train, y_test = self.train_test_split(X, y, stratify=y)

        grid_search_parameters = {
            "tfidf__ngram_range": [(1, 1), (1, 3), (1, 4)],
            "tfidf__use_idf": (True, False),
            "clf__alpha": (0.01, 0.001),
        }

        for model_name, model in [
            ("Bernoulli", BernoulliNB),
            ("multinomial", MultinomialNB),
        ]:

            pipeline = Pipeline(
                [
                    ("norm", TextNormalizer()),
                    (
                        "tfidf",
                        TfidfVectorizer(
                            tokenizer=self._identity, preprocessor=None, lowercase=False
                        ),
                    ),
                    ("clf", model()),
                ]
            )

            gs_model = GridSearchCV(
                pipeline,
                grid_search_parameters,
                scoring=make_scorer(
                    make_index_balanced_accuracy()(geometric_mean_score)
                ),
                verbose=2,
            )

            start_time = time.time()
            gs_model = gs_model.fit(X_train, y_train)
            training_time = f"{int(time.time()-start_time)/60:.1f}"
            best_params = gs_model.best_params_

            with open(
                self._file_path
                / Path(f"../data/results/{entity}_{model_name}_model.pickle"),
                "wb",
            ) as f:
                pickle.dump(gs_model.best_estimator_, f)

            predicted = gs_model.predict(X_test)
            report = classification_report_imbalanced(y_test, predicted)

            with open(
                self._file_path
                / Path(f"../data/results/{entity}_{model_name}_report.txt"),
                "w",
            ) as f:
                f.write(
                    str(best_params)
                    + "\n\n"
                    + report
                    + "\n\n"
                    + f"training time: {training_time} min."
                )

            self.plot_confusion_matrix(
                confusion_matrix(y_test, predicted),
                ["not key", "is key"],
                f"Confusion matrix, {entity} key entity, {model_name} NBC",
            )

            self.plot_learning_curve(
                pipeline.set_params(**best_params),
                f"Learning curve, {entity} key entity, {model_name} NBC",
                X,
                y,
            )

    def plot_confusion_matrix(
        self, cm, target_names, title,
    ):
        """
        Plot a sklearn confusion matrix (cm)

        Citiation
        ---------
        http://scikit-learn.org/stable/auto_examples/model_selection/plot_confusion_matrix.html

        """

        misclass = 1 - np.trace(cm) / float(np.sum(cm))

        plt.figure(figsize=(8, 6))
        plt.imshow(cm, interpolation="nearest", cmap=plt.get_cmap("Blues"))
        plt.title(title)
        plt.colorbar()

        if target_names is not None:
            tick_marks = np.arange(len(target_names))
            plt.xticks(tick_marks, target_names, rotation=45)
            plt.yticks(tick_marks, target_names)

        thresh = cm.max() / 2
        for i, j in product(range(cm.shape[0]), range(cm.shape[1])):
            plt.text(
                j,
                i,
                "{:,}".format(cm[i, j]),
                horizontalalignment="right",
                color="white" if cm[i, j] > thresh else "black",
            )

        plt.ylabel("True label")
        plt.xlabel("Predicted label\nmisclass={:0.2f}".format(misclass))
        plt.tight_layout()
        plt.savefig(
            self._file_path / Path(f"../data/results/{title.replace(' ', '_')}.pdf")
        )

    def plot_learning_curve(
        self, estimator, title, X, y, train_sizes=np.linspace(0.1, 1.0, 5)
    ):
        """
        Generate test and training learning curve.
        """
        _, ax = plt.subplots(1, 1, figsize=(8, 6))

        ax.set_title(title)
        ax.set_xlabel("Training examples")
        ax.set_ylabel("Score")
        train_sizes, train_scores, test_scores = learning_curve(
            estimator,
            X,
            y,
            train_sizes=train_sizes,
            scoring=make_scorer(make_index_balanced_accuracy()(geometric_mean_score)),
            verbose=1,
        )
        pd.DataFrame(
            {
                "train_size": np.array(
                    [[size] * train_scores.shape[1] for size in train_sizes]
                ).reshape(-1),
                "train_score": train_scores.reshape(-1),
                "test_score": test_scores.reshape(-1),
            }
        ).to_csv(
            self._file_path
            / Path(f"../data/results/{title.replace(' ', '_')}_values.csv")
        )
        train_scores_mean = np.mean(train_scores, axis=1)
        train_scores_std = np.std(train_scores, axis=1)
        test_scores_mean = np.mean(test_scores, axis=1)
        test_scores_std = np.std(test_scores, axis=1)

        # Plot learning curve
        ax.grid()
        ax.fill_between(
            train_sizes,
            train_scores_mean - train_scores_std,
            train_scores_mean + train_scores_std,
            alpha=0.1,
            color="r",
        )
        ax.fill_between(
            train_sizes,
            test_scores_mean - test_scores_std,
            test_scores_mean + test_scores_std,
            alpha=0.1,
            color="g",
        )
        ax.plot(train_sizes, train_scores_mean, "o-", color="r", label="Training score")
        ax.plot(
            train_sizes,
            test_scores_mean,
            "o-",
            color="g",
            label="Cross-validation score",
        )
        ax.legend(loc="best")

        plt.tight_layout()
        plt.savefig(
            self._file_path / Path(f"../data/results/{title.replace(' ', '_')}.pdf")
        )

    def _identity(self, text):
        return text


def main():
    trainer = Trainer()
    trainer.train_key_entity_classifications()
    trainer.train_relevance_scoring()


if __name__ == "__main__":
    main()
