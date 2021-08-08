import logging
import pickle
import re
import unicodedata
from glob import glob
from pathlib import Path

from gensim.models.word2vec import Word2Vec
from multiprocessing import cpu_count
from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from tqdm import tqdm

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


class TextNormalizer:
    def __init__(self):
        self.stopwords = set(stopwords.words("english"))
        self.lemmatizer = WordNetLemmatizer()
        self.tag_remover = re.compile(r"<.*?>")

    def is_punct(self, token):
        return all(unicodedata.category(char).startswith("P") for char in token)

    def is_stopword(self, token):
        return token.lower() in self.stopwords

    def normalize(self, sentence):
        return [
            token.lower()
            for token in word_tokenize(sentence)
            if not self.is_punct(token) and not self.is_stopword(token)
        ]

    def transform(self, documents):
        for document in documents:
            for sentence in sent_tokenize(self.tag_remover.sub("", document)):
                yield self.normalize(sentence)


def read_file(path):
    if not "pickle" in path:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    else:
        with open(path, "rb") as f:
            return pickle.load(f)


def main():
    wiki_articles = glob(
        str(Path(__file__).resolve().parent / "../data/wikipedia/*/wiki*")
    )
    epi_articles = glob(
        str(Path(__file__).resolve().parent / "../data/corpus_processed/*/*.pickle")
    )
    articles = wiki_articles + epi_articles

    t = TextNormalizer()

    sentences = [
        sent
        for sent in tqdm(
            t.transform((read_file(path) for path in articles)), total=len(articles)
        )
    ]
    print("Start training word2vec model...")
    model = Word2Vec(sentences, sg=1, hs=1, size=300, workers=cpu_count(), iter=5)
    logging.info("Save")
    embedding_folder = Path(__file__).resolve().parent / Path("data/embeddings")
    embedding_folder.mkdir(parents=True, exist_ok=True)
    model.save(str(embedding_folder / "embeddings_300"))
    print("Embeddings trained and saved.")


if __name__ == "__main__":
    main()
