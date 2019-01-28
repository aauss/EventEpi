import luigi
import pickle
import os
from epitator.annotator import AnnoDoc
from epitator.geoname_annotator import GeonameAnnotator
from epitator.resolved_keyword_annotator import ResolvedKeywordAnnotator
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator

from nlp_surveillance.event_db_preprocessing import event_db
from nlp_surveillance.wikipedia_list_of_countries.scraper import scrape_wikipedia_countries
from nlp_surveillance.wikipedia_list_of_countries.lookup import (abbreviate_wikipedia_country_df,
                                                                 to_translation_dict)
from nlp_surveillance.wikipedia_list_of_countries.clean import clean_wikipedia_country_df
from nlp_surveillance.wikidata_disease_names.wikidata import disease_name_query
from nlp_surveillance.wikidata_disease_names.rki_abbreviations import get_rki_abbreviations
from nlp_surveillance.wikidata_disease_names.lookup import merge_disease_lookup_as_dict
from nlp_surveillance.translate import diseases, countries
from nlp_surveillance.scraper import promed_scraper
from nlp_surveillance.scraper import text_extractor, who_scraper
from nlp_surveillance.classifier import extract_sentence
from utils.my_utils import delete_non_epitator_name_entity_tiers


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
        cleaned_event_db = event_db.read_cleaned()
        with self.output().open('w') as handler:
            pickle.dump(cleaned_event_db, handler)


class RequestDiseaseNamesFromWikiData(LuigiTaskWithDataOutput):

    def output(self):
        return luigi.LocalTarget(format_path('../data/lookup/disease_lookup_without_abbreviation.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        disease_lookup = disease_name_query()
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
        clean_country_lookup = clean_wikipedia_country_df(country_lookup)
        custom_abbreviated_country_lookup = abbreviate_wikipedia_country_df(clean_country_lookup)
        country_lookup = to_translation_dict(custom_abbreviated_country_lookup)

        with self.output().open('w') as handler:
            pickle.dump(country_lookup, handler)


class MergeDiseaseNameLookupWithAbbreviationsOfRKI(LuigiTaskWithDataOutput):
    def requires(self):
        return RequestDiseaseNamesFromWikiData()

    def output(self):
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
        return luigi.LocalTarget(format_path('../data/scraped/scraped_promed.pkl'), format=luigi.format.Nop)

    def run(self):
        promed_urls = promed_scraper.scrape(self.year_to_scrape)
        with self.output().open('w') as handler:
            pickle.dump(promed_urls, handler)


class ScrapeWHO(LuigiTaskWithDataOutput):
    year_to_scrape = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(format_path('../data/scraped/scraped_who.pkl'), format=luigi.format.Nop)

    def run(self):
        who_urls = who_scraper.scrape(self.year_to_scrape)
        with self.output().open('w') as handler:
            pickle.dump(who_urls, handler)


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
            df_to_extract_from = pickle.load(handler)
        df_to_extract_from.URL = df_to_extract_from.URL.apply(text_extractor.extract_cleaned_text_from_url)
        df_to_extract_from = df_to_extract_from.rename(columns={'URL': 'extracted_text'})
        with self.output().open('w') as handler:
            pickle.dump(df_to_extract_from, handler)


class AnnotateDoc(LuigiTaskWithDataOutput):
    source = luigi.Parameter()

    def requires(self):
        return ScrapeFromURLsAndExtractText(self.source)

    def output(self):
        return luigi.LocalTarget(format_path(f'../data/{self.source}/with_annodoc.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            df_with_text = pickle.load(handler)
        # TODO: Change to extracted_text, when rerun url extraction
        df_with_text.extracted_text = df_with_text.extracted_text.apply(lambda x: AnnoDoc(x) if x is not None else x)
        df_with_text = df_with_text.rename(columns={'extracted_text': 'annotated'})
        with self.output().open('w') as handler:
            pickle.dump(df_with_text, handler)


class AnnotateTier(LuigiTaskWithDataOutput):
    source = luigi.Parameter()

    def requires(self):
        return AnnotateDoc(self.source)


class AnnotateDisease(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(format_path(f'../data/{self.source}/disease_tier.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            df_with_anno_doc = pickle.load(handler)
        df_with_anno_doc.annotated = df_with_anno_doc.annotated.apply(lambda x:
                                                                      (delete_non_epitator_name_entity_tiers(
                                                                          x.add_tiers(ResolvedKeywordAnnotator()))
                                                                       if isinstance(x, AnnoDoc) else x))
        with self.output().open('w') as handler:
            pickle.dump(df_with_anno_doc, handler)


class AnnotateCount(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(format_path(f'../data/{self.source}/count_tier.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            df_with_anno_doc = pickle.load(handler)
        df_with_anno_doc.annotated = df_with_anno_doc.annotated.apply(lambda x:
                                                                      (delete_non_epitator_name_entity_tiers(
                                                                          x.add_tiers(CountAnnotator()))
                                                                       if isinstance(x, AnnoDoc) else x))
        with self.output().open('w') as handler:
            pickle.dump(df_with_anno_doc, handler)


class AnnotateGeonames(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(format_path(f'../data/{self.source}/country_tier.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            df_with_anno_doc = pickle.load(handler)
        df_with_anno_doc.annotated = df_with_anno_doc.annotated.apply(lambda x:
                                                                      (delete_non_epitator_name_entity_tiers(
                                                                          x.add_tiers(GeonameAnnotator()))
                                                                       if isinstance(x, AnnoDoc) else x))
        with self.output().open('w') as handler:
            pickle.dump(df_with_anno_doc, handler)


class AnnotateDate(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(format_path(f'../data/{self.source}/date_tier.pkl'), format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            df_with_anno_doc = pickle.load(handler)
        df_with_anno_doc.annotated = df_with_anno_doc.annotated.apply(lambda x:
                                                                      (delete_non_epitator_name_entity_tiers(
                                                                          x.add_tiers(DateAnnotator()))
                                                                       if isinstance(x, AnnoDoc) else x))


        with self.output().open('w') as handler:
            pickle.dump(df_with_anno_doc, handler)


class ExtractSentencesAndLabel(LuigiTaskWithDataOutput):
    to_learn = luigi.Parameter()

    def requires(self):
        if self.to_learn == 'counts':
            return AnnotateCount('event_db')
        elif self.to_learn == 'dates':
            return AnnotateDate('event_db')
        else:
            raise NotImplementedError

    def output(self):
        return luigi.LocalTarget(format_path(f'../data/event_db/{self.to_learn}_with_sentences_and_label.pkl'),
                                 format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            df_with_tiers_to_learn = pickle.load(handler)
        event_db_with_extracted_sentences = extract_sentence.from_entity(df_with_tiers_to_learn, self.to_learn)


        with self.output().open('w') as handler:
            pickle.dump(event_db_with_extracted_sentences, handler)


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
    # luigi.build([AnnotateDoc('event_db')], local_scheduler=True)
    luigi.build([ExtractSentencesAndLabel('dates')], local_scheduler=True)