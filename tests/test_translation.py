import pandas as pd

from nlp_surveillance.translate import countries, diseases
from nlp_surveillance.event_db_preprocessing._clean_diseases import clean_diseases
from nlp_surveillance.pipeline import (MergeDiseaseNameLookupWithAbbreviationsOfRKI,
                                       CleanCountryLookUpAndAddAbbreviations)


def test_disease_translation():
    lookup = MergeDiseaseNameLookupWithAbbreviationsOfRKI().data_output()
    example_diseases_to_translate = pd.DataFrame({'disease_edb': ['Warzee, '
                                                                  'Gonorrhoee, '
                                                                  'BPS',
                                                                  'Ebola']})
    expected_diseases_translation = pd.DataFrame({'disease_edb': ['wart',
                                                                  'gonorrhea',
                                                                  'pertussis',
                                                                  'Ebola virus disease']})
    translated_diseases = diseases.translate(clean_diseases(example_diseases_to_translate), lookup)
    pd.testing.assert_frame_equal(translated_diseases, expected_diseases_translation)


def test_country_translation():
    lookup = CleanCountryLookUpAndAddAbbreviations().data_output()
    example_country_to_translate = pd.DataFrame({'country_edb': ["Italien",
                                                                 "AbD1",
                                                                 "Kongo",
                                                                 "Taiwan",
                                                                 "Niger",
                                                                 'DRC']})
    expected_country_translation = pd.DataFrame({'country_edb': ['Italy',
                                                                 'AbD1',
                                                                 'Democratic Republic of the Congo',
                                                                 'Taiwan oder Republic of China',
                                                                 'Democratic Republic of the Congo']})
    translated_countries = countries.translate(example_country_to_translate, lookup)
    pd.testing.assert_frame_equal(translated_countries, example_country_to_translate)
