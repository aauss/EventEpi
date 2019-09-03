from eventepi.pipeline import *


def test_clean_event_db():
    clean_event_db = CleanEventDB().data_output()
    expected_columns = ['date_of_data', 'count_edb', 'country_edb', 'disease_edb', 'URL']
    assert all(clean_event_db.columns == expected_columns)


def test_disease_names_from_wiki_data():
    disease_df = RequestDiseaseNamesFromWikiData().data_output()
    expected_columns = ['itemLabel_DE', 'itemLabel_EN']
    assert all(disease_df.columns == expected_columns)


def test_country_list_from_wikipedia():
    country_df = ScrapeCountryNamesFromWikipedia().data_output()
    expected_columns = ['state_name_de', 'full_state_name_de', 'translation_state_name',
                        'iso_three_abbreviation', 'iso_two_abbreviation']
    assert all(country_df.columns == expected_columns)


def test_country_lookup():
    lookup = CleanCountryLookUpAndAddAbbreviations().data_output()
    assert lookup['Vereinigte Staaten'] == 'United States'  # Official abbreviation
    assert lookup['VAE'] == 'United Arab Emirates'  # Intuitive but wrong abbreviation
    assert lookup['ARE'] == 'United Arab Emirates'  # Check official abbreviation


def test_disease_lookup():
    lookup = MergeDiseaseNameLookupWithAbbreviationsOfRKI().data_output()
    assert lookup['SAL'] == 'salmonellosis'
    assert lookup['Affenpocken'] == 'monkeypox'
