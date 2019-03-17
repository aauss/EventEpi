from importlib import import_module  # For dynamic imports.
from sacred import Experiment
# from sacred.observers import FileStorageObserver
from sacred.observers import MongoObserver
from sklearn import svm, datasets, model_selection


ex = Experiment('naive_bayes')
# ex.observers.append(
#     FileStorageObserver.create(
#         basedir="."
#     )
# )
ex.observers.append(
    MongoObserver.create(
        db_name="aussinator",
    )
)


@ex.config
def cfg():
    seed = 42
    snote = False
    adasyn = False
    weighting = False


def load(path):
    """Load a class, given its fully qualified path."""
    p, m = path.rsplit('.', 1)
    module = import_module(p)
    class_or_method = getattr(module, m)
    return class_or_method


@ex.capture(prefix="model")  # Prefix to only get the relevant subset of the config.
def get_model(path, hyperparameters):
    Model = load(path)  # Import model class dynamically.
    return Model(**hyperparameters)  # Instantiate with hyperparameters.


@ex.automain  # Use automain to enable command line integration.
def run(snote, adasyn, weighting):
    if snote:
        pass
    X, y = datasets.load_breast_cancer(return_X_y=True)
    X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size=0.2)
    clf = get_model()
    clf.fit(X_train, y_train)
    return clf.score(X_test, y_test)

