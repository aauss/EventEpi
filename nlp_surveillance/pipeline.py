import luigi
import pickle
import os
import pandas as pd
from luigi import format
from tqdm import tqdm
from pickle import UnpicklingError

from nlp_surveillance.my_utils import flatten_list
from nlp_surveillance.event_db_preprocessing import event_db
from nlp_surveillance.wikipedia_list_of_countries import wikipedia
from nlp_surveillance.wikipedia_list_of_countries.lookup import abbreviate_wikipedia_country_df, to_translation_dict
from nlp_surveillance.wikipedia_list_of_countries.clean import clean_wikipedia_country_df
from nlp_surveillance.wikidata_disease_names import wikidata
from nlp_surveillance.wikidata_disease_names.rki_abbreviations import get_rki_abbreviations
from nlp_surveillance.wikidata_disease_names.lookup import merge_disease_lookup_as_dict
from nlp_surveillance.translate import diseases, countries
from nlp_surveillance.scraper import text_extractor, who_scraper, promed_scraper
from nlp_surveillance.classifier import extract_sentence, naive_bayes, summarize


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

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path('../data/event_db/cleaned.pkl'), format=luigi.format.Nop)

    def run(self):
        """Reads the event_db from data folder and applies cleaning steps.

        Event_db.read_cleaned(), if no other path is given, reads the event data
        base from data/rki/edb.csv

        """
        cleaned_event_db = event_db.read_cleaned()
        with self.output().open('w') as handler:
            pickle.dump(cleaned_event_db, handler)


class RequestDiseaseNamesFromWikiData(LuigiTaskWithDataOutput):

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path('../data/lookup/disease_lookup_without_abbreviation.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        """Create disease name dictionary from German to English

        Makes a request to Wikidata to retrieve all disease names in English and
        German.

        """
        disease_lookup = wikidata.disease_name_query()
        with self.output().open('w') as handler:
            pickle.dump(disease_lookup, handler)


class ScrapeCountryNamesFromWikipedia(LuigiTaskWithDataOutput):

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path('../data/lookup/country_lookup_without_abbreviation.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        """Create country name dictionary from German to English

        Makes a request to Wikidata to retrieve all disease names in English and
        German.

        """
        country_lookup = wikipedia.scrape_wikipedia_countries()
        with self.output().open('w') as handler:
            pickle.dump(country_lookup, handler)


class CleanCountryLookUpAndAddAbbreviations(LuigiTaskWithDataOutput):

    def requires(self):
        return ScrapeCountryNamesFromWikipedia()

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path('../data/lookup/country_lookup.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            country_lookup = pickle.load(handler)
        clean_country_lookup = clean_wikipedia_country_df(country_lookup)
        custom_abbreviated_country_lookup = abbreviate_wikipedia_country_df(clean_country_lookup)
        country_lookup = to_translation_dict(custom_abbreviated_country_lookup)

        with self.output().open('w') as handler:
            pickle.dump(country_lookup, handler)


class MergeDiseaseNameLookupWithAbbreviationsOfRKI(LuigiTaskWithDataOutput):
    def requires(self):
        return RequestDiseaseNamesFromWikiData()

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path('../data/lookup/disease_lookup.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            disease_lookup = pickle.load(handler)

        rki_abbreviations = get_rki_abbreviations()
        disease_lookup_with_abbreviations = merge_disease_lookup_as_dict(disease_lookup, rki_abbreviations)

        with self.output().open('w') as handler:
            pickle.dump(disease_lookup_with_abbreviations, handler)


class ApplyControlledVocabularyToEventDB(LuigiTaskWithDataOutput):
    def requires(self):
        return {'disease_lookup': MergeDiseaseNameLookupWithAbbreviationsOfRKI(),
                'country_lookup': CleanCountryLookUpAndAddAbbreviations(),
                'cleaned_event_db': CleanEventDB()}

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path('../data/event_db/with_controlled_vocab.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input()['disease_lookup'].open('r') as handler:
            disease_lookup = pickle.load(handler)
        with self.input()['country_lookup'].open('r') as handler:
            country_lookup = pickle.load(handler)
        with self.input()['cleaned_event_db'].open('r') as handler:
            cleaned_event_db = pickle.load(handler)
        translated_diseases = diseases.translate(cleaned_event_db, disease_lookup)
        translated_countries = countries.translate(translated_diseases, country_lookup)
        with self.output().open('w') as handler:
            pickle.dump(translated_countries, handler)


class ScrapePromed(LuigiTaskWithDataOutput):
    year_to_scrape = luigi.Parameter()

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path(f'../data/scraped/scraped_promed_{self.year_to_scrape}.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        promed_urls = promed_scraper.scrape(self.year_to_scrape)
        with self.output().open('w') as handler:
            pickle.dump(promed_urls, handler)


class ScrapeWHO(LuigiTaskWithDataOutput):
    year_to_scrape = luigi.Parameter()

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path(f'../data/scraped/scraped_who_{self.year_to_scrape}.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        print(self.year_to_scrape)
        who_urls = who_scraper.scrape(self.year_to_scrape)
        with self.output().open('w') as handler:
            pickle.dump(who_urls, handler)


class ScrapeFromURLsAndExtractText(LuigiTaskWithDataOutput):
    source = luigi.Parameter()

    def requires(self):
        if self.source == 'event_db':
            return ApplyControlledVocabularyToEventDB()
        elif self.source == 'promed':
            return ScrapePromed('2018')
        elif self.source == 'who':
            return ScrapeWHO('2018')

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path(f'../data/extracted_texts/{self.source}_extracted_text.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            df_to_extract_from = pickle.load(handler)

        df_to_extract_from['extracted_text'] = (df_to_extract_from.URL
                                                .apply(text_extractor.extract_cleaned_text_from_url))
        with self.output().open('w') as handler:
            pickle.dump(df_to_extract_from, handler)


class ExtractSentencesAndLabel(LuigiTaskWithDataOutput):
    to_learn: str = luigi.Parameter()

    def requires(self):
        return ScrapeFromURLsAndExtractText('event_db')

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path(f'../data/event_db/{self.to_learn}_with_sentences_and_label.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        learn_to_column = {'dates': 'date_of_data', 'counts': 'count_edb'}
        db_entity = learn_to_column[self.to_learn]

        with self.input().open('r') as handler:
            df_with_text = pickle.load(handler)[[db_entity, 'extracted_text']].dropna()

        sentence_label_tuples_per_text: list = (extract_sentence.from_entity(row.extracted_text,
                                                                             self.to_learn,
                                                                             getattr(row, db_entity))
                                                for row in df_with_text.itertuples())
        sentence_label_df = pd.DataFrame(flatten_list(sentence_label_tuples_per_text),
                                         columns=['sentence', 'label'])
        with self.output().open('w') as handler:
            pickle.dump(sentence_label_df, handler)


class TrainNaiveBayes(LuigiTaskWithDataOutput):
    to_learn = luigi.Parameter()

    def requires(self):
        return ExtractSentencesAndLabel(self.to_learn)

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path(f'../data/classifier/{self.to_learn}_naive_bayes_clf.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            to_learn_df = pickle.load(handler)

        classifier, classification_report, confusion_matrix = naive_bayes.learn(to_learn_df)
        print(f'this is the NB classification report {classification_report}')
        print(f'this is the NB confusion matrix {confusion_matrix}')

        with self.output().open('w') as handler:
            pickle.dump(classifier, handler)


class RecommenderLabeling(LuigiTaskWithDataOutput):

    def requires(self):
        return {'who': ScrapeFromURLsAndExtractText('who'),
                'promed': ScrapeFromURLsAndExtractText('promed'),
                'event_db': CleanEventDB()}

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path('../data/recommender/with_label.csv'))

    def run(self):
        with self.input()['who'].open('r') as handler:
            who_urls_and_text = pickle.load(handler)
        with self.input()['promed'].open('r') as handler:
            promed_urls_and_text = pickle.load(handler)
        with self.input()['event_db'].open('r') as handler:
            cleaned_event_db = pickle.load(handler)

        all_scraped = pd.concat([who_urls_and_text, promed_urls_and_text]).reset_index(drop=True)
        urls_as_true_label = cleaned_event_db['URL']
        all_scraped['label'] = all_scraped['URL'].apply(lambda x: x in set(urls_as_true_label))
        scraped_with_label = all_scraped.drop(columns='URL')

        with self.output().open('w') as handler:
            scraped_with_label.to_csv(handler, index=False)


class RecommenderTierAnnotation(LuigiTaskWithDataOutput):

    def requires(self):
        return RecommenderLabeling()

    def output(self):
        """Specifies path for where to pickle output of Luigi job

        Returns:
            luigi.LocalTarget path object
        """
        return luigi.LocalTarget(format_path('../data/recommender/with_entities.pkl'),
                                 format=luigi.format.Nop)

    def run(self):

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


def format_path(path_string):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), path_string))


if __name__ == '__main__':
    # luigi.build([TrainNaiveBayes('dates')], local_scheduler=True)
    # luigi.build([AnnotateDoc('event_db')], local_scheduler=True)
    # luigi.build([ExtractSentencesAndLabel('dates')], local_scheduler=True)
    # luigi.build([ScrapeFromURLsAndExtractText('event_db')], local_scheduler=True)
    # luigi.build([AnnotateDoc('who'), AnnotateDoc('promed')], local_scheduler=True)
    luigi.build([RecommenderTierAnnotation()], local_scheduler=True)
