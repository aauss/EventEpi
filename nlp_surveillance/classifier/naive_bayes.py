import re

from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB, BernoulliNB
from sklearn.utils.class_weight import compute_sample_weight
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
from imblearn.metrics import classification_report_imbalanced
from nltk.tokenize import word_tokenize


def learn(df):
    text_clf = _get_classifier()
    sentences_train, sentences_test, label_train, label_test = _prepare_data(df)
    text_clf.fit(sentences_train, label_train, clf__sample_weight=_balance_labels(label_train))

    """
    text_clf = _get_classifier(classifier_type)
    sentences_train, sentences_test, label_train, label_test = _prepare_data(df)
    text_clf.fit(sentences_train,
                 label_train,
                 clf__sample_weight=_balance_labels(label_train))
    predicted = text_clf.predict(sentences_test)
    classification_report, confusion_matrix = _evaluate(predicted, label_test)
    return text_clf, classification_report, confusion_matrix


def _get_classifier(classifier_type):
    if classifier_type == "multi":
        clf = MultinomialNB()
    else:
        clf = BernoulliNB()
    text_clf = Pipeline([
        ('vect', CountVectorizer(stop_words="english",
                                 tokenizer=word_tokenize)),
        ('tfidf', TfidfTransformer()),
        ('clf', clf)])
    parameters = {'vect__ngram_range': [(1, 1), (1, 2), (1, 3), (1, 4), (1, 5)],
                  'tfidf__use_idf': (True, False),
                  'clf__alpha': (1, 0.5, 0.1, 1e-2, 1e-3)}
    gs_clf = GridSearchCV(text_clf, parameters, iid=False, cv=4)
    return gs_clf


def _prepare_data(df):
    sentences_without_bad_tokenization = df["sentence"].apply(lambda x:
                                                              re.sub(r"([0-9a-zA-Z]+)\.([A-Za-z]+\s)",
                                                                     r"\g<1>. \g<2>",
                                                                     x)
                                                              )
    X_train, X_test, y_train, y_test = train_test_split(sentences_without_bad_tokenization,
                                                        df["label"],
                                                        random_state=42,
                                                        stratify=df["label"],
                                                        )
    return X_train, X_test, y_train, y_test


def _balance_labels(label):
    y_balanced = compute_sample_weight(class_weight='balanced', y=label)
    return y_balanced


def _evaluate(predicted, label_test):
    classification_report = classification_report_imbalanced(label_test, predicted)
    confusion_matrix = metrics.confusion_matrix(label_test, predicted)
    return classification_report, confusion_matrix
