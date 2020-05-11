import logging
import os
import pickle
import time
from pathlib import Path
from typing import List, Optional, Union

import nltk
from bs4 import BeautifulSoup
from nltk import pos_tag, sent_tokenize, word_tokenize
from nltk.corpus.reader.api import CategorizedCorpusReader, CorpusReader
from readability.readability import Document, Unparseable


class HTMLCorpusReader(CategorizedCorpusReader, CorpusReader):
    """A corpus reader for raw HTML documents to enable preprocessing."""

    def __init__(
        self,
        root: Path = (Path(__file__).parent.resolve() / Path("../data/corpus/")),
        fileids: str = r".+\.html",
        encoding: str = "utf8",
        tags=["h1", "h2", "h3", "h4", "h5", "h6", "h7", "p", "li"],
        **kwargs,
    ):
        """Initialize the corpus reader.  Categorization arguments
            (``cat_pattern``, ``cat_map``, and ``cat_file``) are passed to
            the ``CategorizedCorpusReader`` constructor.  The remaining
            arguments are passed to the ``CorpusReader`` constructor.

        Keyword Arguments:
            root {Path} -- Path of corpus root (default: {(Path(__file__).parent.resolve() / Path("../data/corpus/"))})
            fileids {str} -- Regex pattern for documents (default: {r".+\.html"})
            encoding {str} -- (default: {"utf8"})
        """
        # Add the default category pattern if not passed into the class.
        if not any(key.startswith("cat_") for key in kwargs.keys()):
            kwargs["cat_pattern"] = r"([\w_\s]+)\.*"
        # Initialize the NLTK corpus reader objects
        CategorizedCorpusReader.__init__(self, kwargs)
        CorpusReader.__init__(self, str(root), fileids, encoding)

        self.tags = tags
        self.log = logging.getLogger("readability.readability")
        self.log.setLevel("WARNING")

    def resolve(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """Returns a list of fileids or categories depending on what is passed
        to each internal corpus reader function. Implemented similarly to
        the NLTK ``CategorizedPlaintextCorpusReader``.

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        if fileids is not None and categories is not None:
            raise ValueError("Specify fileids or categories, not both")

        if categories is not None:
            return self.fileids(categories)
        return fileids

    def docs(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """Returns the complete text of an HTML document.

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve

        Yields:
            str-- Document from Corpus
        """
        # Resolve the fileids and the categories
        fileids = self.resolve(fileids, categories)

        # Create a generator, loading one document into memory at a time.
        for path, encoding in self.abspaths(fileids, include_encoding=True):
            with open(path, "r", encoding=encoding) as f:
                yield f.read()

    def html(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """Returns the HTML content of each document, made "readable"

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        for doc in self.docs(fileids, categories):
            try:
                yield Document(doc).summary()
            except Unparseable as e:
                print("Could not parse HTML: ", e)
                continue

    def paras(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """Parse HTMLs based on paragraphs using HTML tags."

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        for html in self.html(fileids, categories):
            soup = BeautifulSoup(html, features="lxml")
            for element in soup.find_all(self.tags):
                yield element.text
            soup.decompose()

    def sents(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """Tokenize documents into sentences."

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        for paragraph in self.paras(fileids, categories):
            for sentence in sent_tokenize(paragraph):
                yield sentence

    def words(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """Tokenize sentences of documents into words."

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        for sentence in self.sents(fileids, categories):
            for token in word_tokenize(sentence):
                yield token

    def tokenize(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """
        Segments, tokenizes, and tags a document in the corpus.

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        for paragraph in self.paras(fileids=fileids, categories=categories):
            yield [pos_tag(word_tokenize(sent)) for sent in sent_tokenize(paragraph)]

    def sizes(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """
        Returns a list of tuples, the fileid and size on disk of the file.
        This function is used to detect oddly large files in the corpus.

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve

        Yields:
            int -- Document size
        """
        # Resolve the fileids and the categories
        fileids = self.resolve(fileids, categories)

        # Create a generator, getting every path and computing filesize
        for path in self.abspaths(fileids):
            yield self.human_size(os.path.getsize(path))

    def human_size(self, bytes_, units=[" bytes", "KB", "MB", "GB", "TB", "PB", "EB"]):
        """ Returns a human readable string reprentation of bytes"""
        return (
            str(bytes_) + units[0]
            if bytes_ < 1024
            else self.human_size(bytes_ >> 10, units[1:])
        )

    def describe(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """
        Performs a single pass of the corpus and
        returns a dictionary with a variety of metrics
        concerning the state of the corpus.

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        started = time.time()

        counts = nltk.FreqDist()
        tokens = nltk.FreqDist()

        for para in self.paras(fileids, categories):
            counts["paras"] += 1
            for sent in sent_tokenize(para):
                counts["sents"] += 1
                for word in word_tokenize(sent):
                    counts["words"] += 1
                    tokens[word] += 1

        n_fileids = len(self.resolve(fileids, categories) or self.fileids())
        n_topics = len(self.categories(self.resolve(fileids, categories)))

        return {
            "files": n_fileids,
            "num_categories": n_topics,
            "paragraphs": counts["paras"],
            "sentences": counts["sents"],
            "words": counts["words"],
            "vocabulary": len(tokens),
            "lexical_diversity": float(counts["words"]) / float(len(tokens)),
            "paras_per_doc": float(counts["paras"]) / float(n_fileids),
            "sents_per_para": float(counts["sents"]) / float(counts["paras"]),
            "secs": time.time() - started,
        }


class PickledCorpusReader(CategorizedCorpusReader, CorpusReader):
    def __init__(
        self,
        root: Path = (
            Path(__file__).parent.resolve() / Path("../data/corpus_processed/")
        ),
        fileids=r".+\.pickle",
        **kwargs,
    ):
        """
        Initialize the corpus reader.  Categorization arguments
        (``cat_pattern``, ``cat_map``, and ``cat_file``) are passed to
        the ``CategorizedCorpusReader`` constructor.  The remaining arguments
        are passed to the ``CorpusReader`` constructor.

        Keyword Arguments:
            root {Path} -- Path of corpus root (default: {(Path(__file__).parent.resolve() / Path("../data/corpus_processed/"))})
            fileids {regexp} -- Regex pattern for documents (default: {r".+\.html"})
        """
        # Add the default category pattern if not passed into the class.
        if not any(key.startswith("cat_") for key in kwargs.keys()):
            kwargs["cat_pattern"] = r"([\w_\s]+)\.*"

        CategorizedCorpusReader.__init__(self, kwargs)
        CorpusReader.__init__(self, str(root), fileids)

    def resolve(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """Returns a list of fileids or categories depending on what is passed
        to each internal corpus reader function. Implemented similarly to
        the NLTK ``CategorizedPlaintextCorpusReader``.

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        if fileids is not None and categories is not None:
            raise ValueError("Specify fileids or categories, not both")

        if categories is not None:
            return self.fileids(categories)
        return fileids

    def docs(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """Returns the complete text of an HTML document from a pickle.

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve

        Yields:
            str-- Document from Corpus
        """
        # Resolve the fileids and the categories
        fileids = self.resolve(fileids, categories)

        # Create a generator, loading one document into memory at a time.
        for path, enc, fileid in self.abspaths(fileids, True, True):
            with open(path, "rb") as f:
                yield pickle.load(f)

    def paras(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """
        Returns a generator of paragraphs where each paragraph is a list of
        sentences, which is in turn a list of (token, tag) tuples.
        
        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        for doc in self.docs(fileids, categories):
            for paragraph in doc:
                yield paragraph

    def sents(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """
        Returns a generator of sentences where each sentence is a list of
        (token, tag) tuples.

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        for paragraph in self.paras(fileids, categories):
            for sentence in paragraph:
                yield sentence

    def tagged(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """
        Returns a generator of (token, tag) tuples.

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        for sent in self.sents(fileids, categories):
            for token in sent:
                yield token

    def words(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ):
        """
        Returns a generator of words.

        Arguments:
            fileids {Optional[List[str]]} -- Filenames of corpus documents
            categories {Optional[Union[str, List[str]]]} -- A string denoting the category to resolve
        """
        for token in self.tagged(fileids, categories):
            yield token[0]
