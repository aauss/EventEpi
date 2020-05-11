import os
import pickle
from pathlib import Path
from typing import List, Optional, Tuple, Union

from nltk import pos_tag, sent_tokenize, word_tokenize
from tqdm import tqdm


class CorpusPreprocessor:
    """Wraps HTMLCorpusReader with a preprocessor"""

    def __init__(
        self,
        corpus,
        target: Path = (
            Path(__file__).parent.resolve() / Path("../data/corpus_processed/")
        ),
    ) -> None:
        """Takes a corpus reader which then can be preprocessed and pickled

        Args:
            corpus: A CorpusReader that should be preprocess
            target: Path to root of preprocessed corpus
        """
        self.corpus = corpus
        self.target = target

    def fileids(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ) -> List[str]:
        """Returns fileids filtered by category or fileids

        Args:
            fileids: File ID to return.
            categories: Categories to filter file IDs

        Returns:
            List of requested file IDs.
        """
        fileids = self.corpus.resolve(fileids, categories)
        if fileids:
            return fileids
        return self.corpus.fileids()

    def abspath(self, fileid: str) -> Path:
        """Returns the absolute path to the target fileid from the corpus fileid.

        Args:
            fileid: File ID to which abspath is returned.

        Returns:
            Absolute path of file ID.
        """
        return (self.target.resolve() / Path(fileid)).with_suffix(".pickle")

    def tokenize(self, fileid: str) -> List[List[Tuple[str, str]]]:
        """        
        Segments, tokenizes, and tags a document in the corpus.

        Args:
            fileid: File ID to tokenize.

        Yields:
            List, of sentences containing lists of POS-tagged words per paragraph.
        """
        for paragraph in self.corpus.paras(fileids=fileid):
            yield [pos_tag(word_tokenize(sent)) for sent in sent_tokenize(paragraph)]

    def process(self, fileid: str) -> Path:
        """Tokenizes and pickles a document by fileid.

        Args:
            fileid: A file ID to process.

        Returns:
            Path to processed file.
        """

        target = (self.target.resolve() / Path(fileid)).with_suffix(".pickle")
        parent = target.parent.mkdir(parents=True, exist_ok=True)

        document = list(self.tokenize(fileid))
        with open(target, "wb") as f:
            pickle.dump(document, f, pickle.HIGHEST_PROTOCOL)

        del document
        return target

    def transform(
        self,
        fileids: Optional[List[str]] = None,
        categories: Optional[Union[str, List[str]]] = None,
    ) -> List[str]:
        """
        Transform the wrapped corpus, writing out the segmented, tokenized,
        and part of speech tagged corpus as a pickle to the target directory.

        Args:
            fileids: Filenames of corpus documents.
            categories: A string denoting the category to resolve.

        Returns:
            File IDs of transformed files.
        """
        return [
            self.process(fileid)
            for fileid in tqdm(
                self.fileids(fileids, categories), desc="Transform files into pickles"
            )
        ]
