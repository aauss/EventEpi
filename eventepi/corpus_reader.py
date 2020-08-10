import logging
import pickle
from pathlib import Path
from typing import Iterator, List, Optional

from html2text import HTML2Text
from nltk.corpus.reader.api import CorpusReader
from readability.readability import Document, Unparseable
from tqdm import tqdm


class EpiCorpusReader(CorpusReader):
    """A corpus reader for HTML documents."""

    def __init__(
        self,
        root: Path = (Path(__file__).parent.resolve() / Path("../data/corpus/")),
        target: Path = (
            Path(__file__).parent.resolve() / Path("../data/corpus_processed/")
        ),
        fileids: str = r".+\.html",
        encoding: str = "utf8",
    ) -> None:
        """Initialize the corpus reader.

        Keyword Arguments:
            root {Path} -- Path of corpus root. 
            target {Path} -- Path of transformed corpus root.
            fileids {str} -- Regex pattern for documents.
            encoding {str} -- String encoding of corpus.
        """

        CorpusReader.__init__(self, str(root), fileids, encoding)
        self.target = target

        self.html2text = HTML2Text()
        self.html2text.ignore_links = True
        self.html2text.ignore_images = True
        self.html2text.ignore_tables = True
        self.html2text.ignore_emphasis = True
        self.html2text.unicode_snob = True

        self.log = logging.getLogger("readability.readability")
        self.log.setLevel("WARNING")

    def docs(self, fileids: Optional[List[str]] = None,) -> Iterator[str]:
        """Returns unprocessed HTML documents.

        Arguments:
            fileids -- Filenames of corpus documents.

        Yields:
            Generator of unprocessed documents of corpus.
        """
        if not fileids:
            fileids = self.fileids()
        for path, encoding in self.abspaths(fileids, include_encoding=True):
            with open(path, "r", encoding=encoding) as f:
                yield f.read()

    def readables(
        self, fileids: Optional[List[str]] = None,
    ):
        """Returns readable HTML documents.

        Arguments:
            fileids -- Filenames of corpus documents

        Yields:
            Generator of "readable" documents of corpus
        """
        if not fileids:
            fileids = self.fileids()
        for doc in self.docs(fileids):
            try:
                yield Document(doc).summary()
            except Unparseable as e:
                print("Could not parse HTML: ", e)
                continue

    def texts(
        self, fileids: Optional[List[str]] = None, disable: bool = False,
    ):
        """Returns readable HTML as raw text.

        Arguments:
            fileids -- Filenames of corpus documents.
            disable -- Whether to disable progress bar.

        Yields:
            Generator of raw text documents of corpus.
        """
        if not fileids:
            fileids = self.fileids()
        for doc in tqdm(
            self.readables(fileids),
            total=len(fileids),
            desc="Extract raw text from HTMLs",
            disable=True,
        ):
            yield self.html2text.handle(doc)

    def transform(self) -> None:
        """Transforms corpus to raw text and pickles it."""
        for fileid in tqdm(self.fileids(), desc="Transform and pickle corpus"):
            target = (self.target.resolve() / Path(fileid)).with_suffix(".pickle")
            target.parent.mkdir(parents=True, exist_ok=True)
            raw_document = list(self.texts(fileid, disable=True))[0]
            with open(target, "wb") as f:
                pickle.dump(
                    raw_document, f, pickle.HIGHEST_PROTOCOL,
                )


class PickledCorpusReader(CorpusReader):
    def __init__(
        self,
        root: Path = (
            Path(__file__).parent.resolve() / Path("../data/corpus_processed/")
        ),
        fileids=r".+\.pickle",
    ):
        """
        Initialize the corpus reader.

        Keyword Arguments:
            root -- Path of corpus root.
            fileids -- Regex pattern for documents.
        """
        CorpusReader.__init__(self, str(root), fileids)

    def docs(self, fileids: Optional[List[str]] = None,) -> Iterator[str]:
        """Returns the processes text of a pickled corpus document.

        Arguments:
            fileids -- Filenames of corpus documents.

        Yields:
            Generator of processed documents of corpus.
        """
        if fileids is None:
            fileids = self.fileids()

        for path, enc in self.abspaths(fileids, include_encoding=True):
            with open(path, "rb") as f:
                yield pickle.load(f)
