import pandas as pd

from nlp_surveillance.translate import countries, diseases
from nlp_surveillance.event_db_preprocessing._clean_diseases import clean_diseases
from nlp_surveillance.pipeline import (MergeDiseaseNameLookupWithAbbreviationsOfRKI,
                                       CleanCountryLookUpAndAddAbbreviations)


def test_disease_translation():
    lookup = MergeDiseaseNameLookupWithAbbreviationsOfRKI().data_output()
    example_diseases_to_clean = pd.DataFrame({'disease_edb': ['Warzee, Gonorrhoee, BPS', 'Ebola']})
    expected_diseases_clean = pd.DataFrame({'disease_edb': ['wart', 'gonorrhea', 'pertussis', 'Ebola virus disease']})
    translated_diseases = diseases.translate(clean_diseases(example_diseases_to_clean), lookup)
    pd.testing.assert_frame_equal(translated_diseases, expected_diseases_clean
                                  )

def test_country_translation():
    pass