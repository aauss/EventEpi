import pandas as pd

from eventepi.event_db_preprocessing import translate_diseases, translate_countries
from eventepi.event_db_preprocessing.clean_diseases import clean_diseases
from eventepi.pipeline import (MergeDiseaseNameLookupWithAbbreviationsOfRKI,
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
    translated_diseases = translate_diseases.translate(clean_diseases(example_diseases_to_translate), lookup)
    pd.testing.assert_frame_equal(translated_diseases, expected_diseases_translation)


def test_country_translation():
    lookup = CleanCountryLookUpAndAddAbbreviations().data_output()
    example_country_to_translate = pd.DataFrame({'country_edb': ["Italien",
                                                                 "AbD1",
                                                                 "Kongo",
                                                                 "Taiwan",
                                                                 "Niger",
                                                                 "DRC"]})
    expected_country_translation = pd.DataFrame({'country_edb': ["Italy",
                                                                 "AbD1",
                                                                 "Democratic Republic of the Congo",
                                                                 "Taiwan oder Republic of China",
                                                                 "Niger",
                                                                 "Democratic Republic of the Congo"]})
    translated_countries = translate_countries.translate(example_country_to_translate, lookup)
    pd.testing.assert_frame_equal(translated_countries, expected_country_translation)
