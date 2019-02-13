from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from sklearn.utils.class_weight import compute_sample_weight
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords


def learn(df):
    text_clf = _get_classifier()
    sentences_train, sentences_test, label_train, label_test = _prepare_data(df)
    text_clf.fit(sentences_train, label_train, clf__sample_weight=_balance_labels(label_train))

    predicted = text_clf.predict(sentences_test)
    classification_report, confusion_matrix = _evaluate(predicted, label_test)
    return text_clf, classification_report, confusion_matrix


def _get_classifier():
    text_clf = Pipeline([
        ('vect', CountVectorizer()),
        ('tfidf', TfidfTransformer()),
        ('clf', MultinomialNB())])
    parameters = {'vect__ngram_range': [(1, 1), (1, 2)],
                  'tfidf__use_idf': (True, False),
                  'clf__alpha': (1e-2, 1e-3)}
    gs_clf = GridSearchCV(text_clf, parameters, iid=False, cv=4)
    return gs_clf


def _prepare_data(df):
    sentences = df.sentence.apply(remove_stop_words)
    X_train, X_test, y_train, y_test = train_test_split(sentences, df.label,
                                                        random_state=42)
    return X_train, X_test, y_train, y_test


def _balance_labels(label):
    y_balanced = compute_sample_weight(class_weight='balanced', y=label)
    return y_balanced


def _evaluate(predicted, label_test):
    classification_report = metrics.classification_report(label_test, predicted)
    confusion_matrix = metrics.confusion_matrix(label_test, predicted)
    return classification_report, confusion_matrix


def remove_stop_words(text):
    stopwords_to_remove = set(stopwords.words('english'))
    words_to_process = set(word_tokenize(text))
    text_without_stopwords = words_to_process - stopwords_to_remove
    text_without_stopwords = ' '.join(text_without_stopwords)
    return text_without_stopwords
