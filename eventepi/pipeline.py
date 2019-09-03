import pickle
import os
import logging

import luigi
import pandas as pd

from luigi import format
from tqdm import tqdm
from pickle import UnpicklingError

from eventepi.my_utils import flatten_list
from eventepi.event_db_preprocessing import event_db, translate_diseases, translate_countries
from eventepi.scraper._country_lookup import abbreviate_wikipedia_country_df, to_translation_dict
from eventepi.scraper._clean_wikipedia_table import clean_wikipedia_country_df
from eventepi.scraper._rki_abbreviations import get_rki_abbreviations
from eventepi.scraper._disease_lookup import merge_disease_lookup_as_dict
from eventepi.scraper import text_extractor, who_scraper, promed_scraper, wikidata_diseases, wikipedia_countries
from eventepi.classifier import extract_sentence, naive_bayes, summarize
from eventepi import my_utils


class LuigiTaskWithDataOutput(luigi.Task):

    def data_output(self):
        """Method to use luigi.LocalTarget (path of Luigi job output) to load it


        Returns:
            Some data that has been pickled during Luigi jobs

        """
        with self.output().open('r') as handler:
            try:
                data = pickle.load(handler)
            except UnpicklingError:
                data = pd.read_csv(handler, encoding="utf-8")
        return data


class CleanEventDB(LuigiTaskWithDataOutput):

    def output(self) -> luigi.LocalTarget:
        """Specifies path for where to pickle output of Luigi job

        Returns:
            Path object of pickled Task
        """
        return luigi.LocalTarget(format_path('../data/event_db/cleaned.pkl'), format=luigi.format.Nop)

    def run(self):
        """Reads the event_db from data folder and applies cleaning steps.

        Event_db.read_cleaned(), if no other path is given, reads the event data
        base from data/rki/idb.csv

        """
        cleaned_event_db = event_db.read_cleaned()
        with self.output().open('w') as handler:
            pickle.dump(cleaned_event_db, handler)


class RequestDiseaseNamesFromWikiData(LuigiTaskWithDataOutput):

    def output(self) -> luigi.LocalTarget:
        """Specifies path for where to pickle output of Luigi job

        Returns:
            Path object of pickled Task
        """
        return luigi.LocalTarget(format_path('../data/lookup/disease_lookup_without_abbreviation.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        """Makes request for German and English disease names from Wikidata
        """

        disease_lookup = wikidata_diseases.disease_name_query(proxy)
        with self.output().open('w') as handler:
            pickle.dump(disease_lookup, handler)


class ScrapeCountryNamesFromWikipedia(LuigiTaskWithDataOutput):

    def output(self) -> luigi.LocalTarget:
        """Specifies path for where to pickle output of Luigi job

        Returns:
            Path object of pickled Task
        """
        return luigi.LocalTarget(format_path('../data/lookup/country_lookup_without_abbreviation.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        """Scrapes Wikipedia article 'Liste der Staaten der Erde' (list of sovereign states) to lookup
        between English and German country names
        """

        country_lookup = wikipedia_countries.scrape_wikipedia_countries(proxy=proxy)
        with self.output().open('w') as handler:
            pickle.dump(country_lookup, handler)


class CleanCountryLookUpAndAddAbbreviations(LuigiTaskWithDataOutput):

    def requires(self):
        """Makes dependency in Luigi pipeline for ScrapeCountryNamesFromWikipedia

        Returns:
            ScrapeCountryNamesFromWikipedia() as Task
        """
        return ScrapeCountryNamesFromWikipedia()

    def output(self) -> luigi.LocalTarget:
        """Specifies path for where to pickle output of Luigi job

        Returns:
            Path object of pickled Task
        """
        return luigi.LocalTarget(format_path('../data/lookup/country_lookup.pkl'), format=luigi.format.Nop)

    def run(self):
        """Cleans Wikipedia list of sovereign states, produces 'intuitive' abbreviations and combines them to a lookup
        """

        with self.input().open('r') as handler:
            country_lookup = pickle.load(handler)
        clean_country_lookup = clean_wikipedia_country_df(country_lookup)
        custom_abbreviated_country_lookup = abbreviate_wikipedia_country_df(clean_country_lookup)
        country_lookup = to_translation_dict(custom_abbreviated_country_lookup)

        with self.output().open('w') as handler:
            pickle.dump(country_lookup, handler)


class MergeDiseaseNameLookupWithAbbreviationsOfRKI(LuigiTaskWithDataOutput):
    def requires(self):
        """Makes dependency in Luigi pipeline for RequestDiseaseNamesFromWikiData

        Returns:
            RequestDiseaseNamesFromWikiData() as Task
        """
        return RequestDiseaseNamesFromWikiData()

    def output(self) -> luigi.LocalTarget:
        """Specifies path for where to pickle output of Luigi job

        Returns:
            Path object of pickled Task
        """
        return luigi.LocalTarget(format_path('../data/lookup/disease_lookup.pkl'), format=luigi.format.Nop)

    def run(self):
        """Merges output of Wikidata translation look_up and RKI internal abbreviations look_up
        to a single translation dict
        """
        with self.input().open('r') as handler:
            disease_lookup = pickle.load(handler)

        rki_abbreviations = get_rki_abbreviations()
        disease_lookup_with_abbreviations = merge_disease_lookup_as_dict(disease_lookup, rki_abbreviations)

        with self.output().open('w') as handler:
            pickle.dump(disease_lookup_with_abbreviations, handler)


class ApplyControlledVocabularyToEventDB(LuigiTaskWithDataOutput):

    def requires(self):
        """Makes dependency in Luigi pipeline for several Luigi pipeline components

        Returns (dict):
             Dict for required Luigi pipeline components
        """
        return {'disease_lookup': MergeDiseaseNameLookupWithAbbreviationsOfRKI(),
                'country_lookup': CleanCountryLookUpAndAddAbbreviations(),
                'cleaned_event_db': CleanEventDB()}

    def output(self) -> luigi.LocalTarget:
        """Specifies path for where to pickle output of Luigi job

        Returns:
            Path object of pickled Task
        """
        return luigi.LocalTarget(format_path('../data/event_db/with_controlled_vocab.pkl'), format=luigi.format.Nop)

    def run(self):
        """Takes the disease and country lookup to transform the vocabulary of the incident database to
        controlled vocabulary, i.e., translate German unformatted disease and country names to formatted English
        names that match the output of EpiTator.
        """
        with self.input()['disease_lookup'].open('r') as handler:
            disease_lookup = pickle.load(handler)
        with self.input()['country_lookup'].open('r') as handler:
            country_lookup = pickle.load(handler)
        with self.input()['cleaned_event_db'].open('r') as handler:
            cleaned_event_db = pickle.load(handler)
        logging.info("Start translating diseases")
        translated_diseases = translate_diseases.translate(cleaned_event_db, disease_lookup)
        logging.info("Start translating countries")
        translated_countries = translate_countries.translate(translated_diseases, country_lookup)
        with self.output().open('w') as handler:
            pickle.dump(translated_countries, handler)


class ScrapePromed(LuigiTaskWithDataOutput):
    year_to_scrape: int = luigi.Parameter()

    def output(self) -> luigi.LocalTarget:
        """Specifies path for where to pickle output of Luigi job

        Returns:
            Path object of pickled Task
        """
        return luigi.LocalTarget(format_path(f'../data/scraped/scraped_promed_{self.year_to_scrape}.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        """Scrapes ProMED Mail articles given time range
        """
        promed_urls = promed_scraper.scrape(self.year_to_scrape, proxy=proxy)
        with self.output().open('w') as handler:
            pickle.dump(promed_urls, handler)


class ScrapeWHO(LuigiTaskWithDataOutput):
    year_to_scrape: int = luigi.Parameter()

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path(f'../data/scraped/scraped_who_{self.year_to_scrape}.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        """Scrapes WHO DONs given time range
        """
        who_urls = who_scraper.scrape(self.year_to_scrape, proxy=proxy)
        with self.output().open('w') as handler:
            pickle.dump(who_urls, handler)


class ScrapeFromURLsAndExtractText(LuigiTaskWithDataOutput):
    source = luigi.Parameter()

    def requires(self):
        """Makes dependency in Luigi pipeline for several Luigi pipeline components

        Returns (dict):
             Dict for required Luigi pipeline components
        """
        if self.source == 'event_db':
            return ApplyControlledVocabularyToEventDB()
        elif self.source == 'promed':
            return ScrapePromed('2018')
        elif self.source == 'who':
            return ScrapeWHO('2018')

    def output(self) -> luigi.LocalTarget:
        """Specifies path for where to pickle output of Luigi job

        Returns:
            Path object of pickled Task
        """
        return luigi.LocalTarget(format_path(f'../data/extracted_texts/{self.source}_extracted_text.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        """Takes a DataFrame with an URL column and adds the extracted text from boilerpipe to it
        """
        with self.input().open('r') as handler:
            df_to_extract_from = pickle.load(handler)
        df_to_extract_from = df_to_extract_from[df_to_extract_from["URL"].notna()]
        tqdm.pandas()
        df_to_extract_from['extracted_text'] = (df_to_extract_from["URL"]
                                                .progress_apply(
            lambda x: text_extractor.extract_cleaned_text_from_url(x, proxy=proxy))
                                                )
        with self.output().open('w') as handler:
            pickle.dump(df_to_extract_from, handler)


class ExtractSentencesAndLabel(LuigiTaskWithDataOutput):
    to_learn: str = luigi.Parameter()

    def requires(self):
        """Makes dependency in Luigi pipeline for ScrapeFromURLsAndExtractText

        Returns:
            ScrapeFromURLsAndExtractText() as Task
        """
        return ScrapeFromURLsAndExtractText('event_db')

    def output(self) -> luigi.LocalTarget:
        """Specifies path for where to pickle output of Luigi job

        Returns:
            Path object of pickled Task
        """
        return luigi.LocalTarget(format_path(f'../data/event_db/{self.to_learn}_with_sentences_and_label.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        """Extracts sentences from texts in the incident database that contain the entity==to_learn.
        If the entity found in the sentence matches the entity found in the incident database, the
        sentence is labeled true and false otherwise.

        Returns:
            A pickled DataFrame with sentences that contain an entity==to_learn and a label
            whether the label matches the entity found for this text in the incident database

        """

        learn_to_column = {'dates': 'date_of_data', 'counts': 'count_edb'}
        db_entity = learn_to_column[self.to_learn]

        with self.input().open('r') as handler:
            df_with_text = pickle.load(handler)[[db_entity, 'extracted_text']].dropna()

        sentence_label_tuples_per_text = (extract_sentence.from_entity(row.extracted_text,
                                                                       self.to_learn,
                                                                       getattr(row, db_entity)
                                                                       )
                                          for row in df_with_text.itertuples())
        sentence_label_df = pd.DataFrame(flatten_list(sentence_label_tuples_per_text),
                                         columns=['sentence', 'label'])
        with self.output().open('w') as handler:
            pickle.dump(sentence_label_df, handler)


class TrainNaiveBayes(LuigiTaskWithDataOutput):
    to_learn: str = luigi.Parameter()  # "dates" or "counts"
    classifier_type: str = luigi.Parameter()  # "multi" or "bernoulli"

    def requires(self):
        """Makes dependency in Luigi pipeline for ExtractSentencesAndLabel

        Returns:
            ExtractSentencesAndLabel() as Task
        """
        return ExtractSentencesAndLabel(self.to_learn)

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(
            format_path(f'../data/classifier/{self.to_learn}_{self.classifier_type}naive_bayes_clf.pkl'),
            format=luigi.format.Nop)

    def run(self):
        """Trains a naive Bayes classifier on sentences containing to_learn={count, dates} entities
        where the corresponding boolean label says whether the sentence belongs to a sentence
        that was entered into the incident database (key count/date of a text)
        """
        with self.input().open('r') as handler:
            to_learn_df = pickle.load(handler)
        classifier, classification_report, confusion_matrix = naive_bayes.train(to_learn_df,
                                                                                self.classifier_type)
        print(f'This is the naive Bayes classification report {classification_report}')
        print(f'This is the naive confusion matrix {confusion_matrix}')
        classifier_report_dict = {f"{self.classifier_type}_classifier": classifier,
                                  "report": classification_report,
                                  "confusion_matrix": confusion_matrix}

        with self.output().open('w') as handler:
            pickle.dump(classifier_report_dict, handler)


class RecommenderLabeling(LuigiTaskWithDataOutput):

    def requires(self):
        """Makes dependency in Luigi pipeline for several Luigi pipeline components

        Returns (dict):
             Dict for required Luigi pipeline components
        """
        return {'who': ScrapeFromURLsAndExtractText('who'),
                'promed': ScrapeFromURLsAndExtractText('promed'),
                'event_db': CleanEventDB()}

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path('../data/recommender/with_label.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        """Takes all WHO DON and ProMED Mail article URLs and labels the URLs of the incident database
        """
        with self.input()['who'].open('r') as handler:
            who_urls_and_text = pickle.load(handler)
        with self.input()['promed'].open('r') as handler:
            promed_urls_and_text = pickle.load(handler)
        with self.input()['event_db'].open('r') as handler:
            cleaned_event_db = pickle.load(handler)

        all_scraped = pd.concat([who_urls_and_text, promed_urls_and_text]).reset_index(drop=True)
        urls_as_true_label = set(cleaned_event_db['URL'])
        all_scraped['label'] = all_scraped['URL'].apply(lambda x: x in urls_as_true_label)
        scraped_with_label = all_scraped.drop(columns='URL')
        with self.output().open('w') as handler:
            pickle.dump(scraped_with_label, handler)


class RecommenderTierAnnotation(LuigiTaskWithDataOutput):

    def requires(self):
        """Makes dependency in Luigi pipeline for RecommenderLabeling

        Returns:
            RecommenderLabeling() as Task
        """
        return RecommenderLabeling()

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path('../data/recommender/with_entities.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        """Extract EpiTator TierAnnotations that best summarize an epidemiological article.

        The key disease and country is chosen based on the most frequent disease/country in the text.
        For the key date/count extraction, we use the most probable date/count entity belonging to the class
        "key information" using a naive Bayes classifier trained on the incident database
        """
        with self.input().open('r') as handler:
            text_with_label = pd.read_csv(handler)
            texts = text_with_label['extracted_text'].tolist()
            label = text_with_label['label'].tolist()

        dicts = []
        for text in tqdm(texts):
            dicts.append(summarize.annotate_and_summarize
                         (text,
                          clf_dates=TrainNaiveBayes('dates').data_output(),
                          clf_counts=TrainNaiveBayes('counts').data_output(),
                          ))

        entities_and_label = pd.concat([pd.DataFrame(dicts), pd.DataFrame({'label': label, 'text': texts})], axis=1)
        entities_and_label_unpacked = entities_and_label.applymap(lambda x: x[0] if isinstance(x, list) else x)

        with self.output().open('w') as handler:
            pickle.dump(entities_and_label_unpacked, handler)


def format_path(path_string: str) -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), path_string))


if __name__ == '__main__':
    global proxy
    if not my_utils.connection_is_possible():
        my_utils.assure_right_proxy_settings()
        headers = my_utils.load_rki_header_and_proxy_dict()['headers']
        proxy = my_utils.load_rki_header_and_proxy_dict()['proxy']
        proxy = dict(zip(["http", "https"], proxy.values()))
    # luigi.build([TrainNaiveBayes('dates')], local_scheduler=True)
    # luigi.build([ExtractSentencesAndLabel('dates')], local_scheduler=True)
    luigi.build([TrainNaiveBayes('dates', 'bernoulli')], local_scheduler=True)
    # luigi.build([RecommenderTierAnnotation()], local_scheduler=True)
