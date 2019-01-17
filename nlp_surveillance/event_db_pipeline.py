import luigi
import pickle
import os

from nlp_surveillance.event_db_preprocessing import event_db
from nlp_surveillance.wikipedia_list_of_countries.scraper import scrape_wikipedia_countries
from nlp_surveillance.wikipedia_list_of_countries.cleaner import clean_wikipedia_countries
from nlp_surveillance.wikipedia_list_of_countries.custom_abbreviations import abbreviate_wikipedia_country_df
from nlp_surveillance.wikidata_disease_names.query import get_wikidata_disease_df
from nlp_surveillance.wikidata_disease_names.rki_abbreviations import get_rki_abbreviations
from nlp_surveillance.wikidata_disease_names.lookup import merge_disease_lookup


class LuigiTaskWithDataOutput(luigi.Task):
    # A class that allows my task to output data

    def data_output(self):
        with self.output().open('r') as handler:
            data = pickle.load(handler)
        return data


class CleanEventDB(LuigiTaskWithDataOutput):

    def output(self):
        return luigi.LocalTarget(format_path('../data/event_db/cleaned.pkl'), format=luigi.format.Nop)

    def run(self):
        print(__file__, '======================================')
        cleaned_event_db = event_db.read_cleaned()
        with self.output().open('w') as handler:
            pickle.dump(cleaned_event_db, handler)


class RequestDiseaseNamesFromWikiData(LuigiTaskWithDataOutput):

    def output(self):
        return luigi.LocalTarget(format_path('../data/lookup/disease_lookup_without_abbreviation.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        disease_lookup = get_wikidata_disease_df()
        with self.output().open('w') as handler:
            pickle.dump(disease_lookup, handler)


class ScrapeCountryNamesFromWikipedia(LuigiTaskWithDataOutput):

    def output(self):
        return luigi.LocalTarget(format_path('../data/lookup/country_lookup_without_abbreviation.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        country_lookup = scrape_wikipedia_countries()
        with self.output().open('w') as handler:
            pickle.dump(country_lookup, handler)


class CleanCountryLookUpAndAddAbbreviations(LuigiTaskWithDataOutput):
    def requires(self):
        return ScrapeCountryNamesFromWikipedia()

    def output(self):
        return luigi.LocalTarget(format_path('../data/lookup/country_lookup.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            country_lookup = pickle.load(handler)
        clean_country_lookup = clean_wikipedia_countries(country_lookup)
        custom_abbreviated_country_lookup = abbreviate_wikipedia_country_df(clean_country_lookup)

        with self.output().open('w') as handler:
            pickle.dump(custom_abbreviated_country_lookup, handler)


class MergeDiseaseNameLookupWithAbbreviationsOfRKI(LuigiTaskWithDataOutput):
    def requires(self):
        return RequestDiseaseNamesFromWikiData()

    def output(self):
        return luigi.LocalTarget(format_path('../data/lookup/disease_lookup.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            disease_lookup = pickle.load(handler)

        rki_abbreviations = get_rki_abbreviations()
        disease_lookup_with_abbreviations = merge_disease_lookup(disease_lookup, rki_abbreviations)

        with self.output().open('w') as handler:
            pickle.dump(disease_lookup_with_abbreviations, handler)


class ApplyControlledVocabularyToEventDB(LuigiTaskWithDataOutput):
    def requires(self):
        return {'disease_lookup': MergeDiseaseNameLookupWithAbbreviationsOfRKI(),
                'country_lookup': CleanCountryLookUpAndAddAbbreviations(),
                'cleaned_event_db': CleanEventDB()}

    def output(self):
        return luigi.LocalTarget(format_path('../data/event_db/with_controlled_vocab.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input()['disease_lookup'].open('r') as handler:
            disease_lookup = pickle.load(handler)
        with self.input()['country_lookup'].open('r') as handler:
            country_lookup = pickle.load(handler)
        with self.input()['cleaned_event_db'].open('r') as handler:
            cleaned_event_db = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class ScrapePromed(LuigiTaskWithDataOutput):
    year_to_scrape = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(format_path('../data/scraped/scraped_promed.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class ScrapeWHO(LuigiTaskWithDataOutput):
    year_to_scrape = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(format_path('../data/scraped/scraped_who.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class ScrapeFromURLsAndExtractText(LuigiTaskWithDataOutput):
    source = luigi.Parameter()

    def requires(self):
        if self.source == 'event_db':
            return ApplyControlledVocabularyToEventDB()
        elif self.source == 'promed':
            return ScrapePromed(2018)
        elif self.source == 'who':
            return ScrapeWHO(2018)

    def output(self):
        return luigi.LocalTarget(format_path(f'../data/extracted_texts/{self.source}_extracted_text.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateDoc(LuigiTaskWithDataOutput):
    source = luigi.Parameter()

    def requires(self):
        return ScrapeFromURLsAndExtractText(self.source)

    def output(self):
        return luigi.LocalTarget(f'../data/{self.source}/with_annodoc.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateTier(LuigiTaskWithDataOutput):
    source = luigi.Parameter()

    def requires(self):
        return AnnotateDoc(self.source)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateDisease(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(f'../data/{self.source}/disease_tier.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateCount(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(f'../data/{self.source}/count_tier.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateCountry(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(f'../data/{self.source}/country_tier.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateDate(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(f'../data/{self.source}/date_tier.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class ExtractSentencesAndLabel(LuigiTaskWithDataOutput):
    to_learn = luigi.Parameter()

    def requires(self):
        if self.to_learn == 'count':
            return AnnotateCount('event_db')
        elif self.to_learn == 'date':
            return AnnotateDate('event_db')

    def output(self):
        return luigi.LocalTarget(f'../data/event_db/{self.to_learn}_with_sentences_and_label.pkl',
                                 format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class TrainNaiveBayes(LuigiTaskWithDataOutput):
    to_learn = luigi.Parameter()

    def requires(self):
        return ExtractSentencesAndLabel(self.to_learn)

    def output(self):
        return luigi.LocalTarget(f'../data/classifier/{self.to_learn}_naive_bayes_clf.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


def format_path(path_string):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), path_string))

if __name__ == '__main__':
    # luigi.build([TrainNaiveBayes('date')], local_scheduler=True)
    luigi.build([CleanEventDB()], local_scheduler=True)
