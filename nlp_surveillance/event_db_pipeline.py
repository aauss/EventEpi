import luigi
import pickle

import event_db


class CleanEventDB(luigi.Task):

    def output(self):
        return luigi.LocalTarget('data/event_db/cleaned.pkl', format=luigi.format.Nop)

    def run(self):
        with self.output().open('w') as handler:
            cleaned_event_db = event_db.read_cleaned()
            pickle.dump(cleaned_event_db, handler)


class RequestDiseaseNamesFromWikiData(luigi.Task):

    def output(self):
        return luigi.LocalTarget('data/disease_lookup_without_abbreviation.pkl', format=luigi.format.Nop)

    def run(self):
        with self.output().open('w') as handler:
            TOWRITE = 'lol'
            pickle.dump(TOWRITE, handler)


class ScrapeCountryNamesFromWikipedia(luigi.Task):

    def output(self):
        return luigi.LocalTarget('data/country_lookup_without_abbreviation.pkl', format=luigi.format.Nop)

    def run(self):
        with self.output().open('w') as handler:
            TOWRITE = 'lol'
            pickle.dump(TOWRITE, handler)


class CleanCountryLookUpAndAddAbbreviations(luigi.Task):
    def requires(self):
        return ScrapeCountryNamesFromWikipedia()

    def output(self):
        return luigi.LocalTarget('data/country_lookup.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class MergeDiseaseNameLookupWithDiseaseCodeOfRKI(luigi.Task):
    def requires(self):
        return RequestDiseaseNamesFromWikiData()

    def output(self):
        return luigi.LocalTarget('data/disease_lookup.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class ApplyControlledVocabularyToEventDB(luigi.Task):
    def requires(self):
        return {'disease_lookup': MergeDiseaseNameLookupWithDiseaseCodeOfRKI(),
                'country_lookup': CleanCountryLookUpAndAddAbbreviations(),
                'cleaned_event_db': CleanEventDB()}

    def output(self):
        return luigi.LocalTarget('data/event_db/with_controlled_vocab.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input()['disease_lookup'].open('r') as handler:
            TOREAD = pickle.load(handler)
        with self.input()['country_lookup'].open('r') as handler:
            TOREAD = pickle.load(handler)
        with self.input()['cleaned_event_db'].open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class ScrapePromed(luigi.Task):
    year_to_scrape = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/scraped_promed.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class ScrapeWHO(luigi.Task):
    year_to_scrape = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/scraped_who.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class ScrapeFromURLsAndExtractText(luigi.Task):
    source = luigi.Parameter()

    def requires(self):
        if self.source == 'event_db':
            return ApplyControlledVocabularyToEventDB()
        elif self.source == 'promed':
            return ScrapePromed(2018)
        elif self.source == 'who':
            return ScrapeWHO(2018)

    def output(self):
        return luigi.LocalTarget(f'data/{self.source}extracted_text.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateDoc(luigi.Task):
    source = luigi.Parameter()

    def requires(self):
        return ScrapeFromURLsAndExtractText(self.source)

    def output(self):
        return luigi.LocalTarget(f'data/{self.source}/with_annodoc.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateTier(luigi.Task):
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
        return luigi.LocalTarget(f'data/{self.source}/disease_tier.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateCount(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(f'data/{self.source}/count_tier.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateCountry(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(f'data/{self.source}/country_tier.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class AnnotateDate(AnnotateTier):

    def output(self):
        return luigi.LocalTarget(f'data/{self.source}/date_tier.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class ExtractSentencesAndLabel(luigi.Task):
    to_learn = luigi.Parameter()

    def requires(self):
        if self.to_learn == 'count':
            return AnnotateCount('event_db')
        elif self.to_learn == 'date':
            return AnnotateDate('event_db')

    def output(self):
        return luigi.LocalTarget(f'data/event_db/{self.to_learn}_with_sentences_and_label.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


class TrainNaiveBayes(luigi.Task):
    to_learn = luigi.Parameter()

    def requires(self):
        return ExtractSentencesAndLabel(self.to_learn)

    def output(self):
        return luigi.LocalTarget(f'data/{self.to_learn}_naive_bayes_clf.pkl', format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as handler:
            TOREAD = pickle.load(handler)
        # TODO: do stuff lol
        with self.output().open('w') as handler:
            TOWRITE = None
            pickle.dump(TOWRITE, handler)


if __name__ == '__main__':
    luigi.build([TrainNaiveBayes('date')], local_scheduler=True)
