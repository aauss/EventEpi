import pandas as pd

from nlp_surveillance.pipeline import ScrapeCountryNamesFromWikipedia
from nlp_surveillance.wikipedia_list_of_countries.clean import clean_wikipedia_country_df
from nlp_surveillance.wikipedia_list_of_countries.lookup import abbreviate_wikipedia_country_df


def test_scrape_country_names_from_wikipedia():
    wikipedia_country_df = ScrapeCountryNamesFromWikipedia().data_output()
    example_row_1 = wikipedia_country_df.iloc[94]
    expected_row_1 = pd.Series({'state_name_de': 'Kosovo[9]',
                                'full_state_name_de': 'Republik Kosovo[10]',
                                'translation_state_name': 'Kosovo',
                                'iso_three_abbreviation': 'XXK[13]',
                                'iso_two_abbreviation': 'XK'}, name=94)
    pd.testing.assert_series_equal(expected_row_1, example_row_1)

    example_row_2 = wikipedia_country_df.iloc[93]
    expected_row_2 = pd.Series({'state_name_de': 'Korea, Süd',
                                'full_state_name_de': 'Republik Korea',
                                'translation_state_name': 'Korea, Republic of (South Korea)',
                                'iso_three_abbreviation': 'KOR',
                                'iso_two_abbreviation': 'KR'}, name=93)
    pd.testing.assert_series_equal(example_row_2, expected_row_2)


def test_clean_wikipedia_countries():
    wikipedia_country_df = clean_wikipedia_country_df(ScrapeCountryNamesFromWikipedia().data_output())
    example_row_1 = wikipedia_country_df.iloc[94]
    expected_row_1 = pd.Series({'state_name_de': 'Kosovo',
                                'full_state_name_de': 'Republik Kosovo',
                                'translation_state_name': 'Kosovo',
                                'iso_three_abbreviation': 'XXK',
                                'iso_two_abbreviation': 'XK'}, name=94)
    pd.testing.assert_series_equal(example_row_1, expected_row_1)

    example_row_2 = wikipedia_country_df.iloc[93]
    expected_row_2 = pd.Series({'state_name_de': 'Süd Korea',
                                'full_state_name_de': 'Republik Korea',
                                'translation_state_name': 'Republic of Korea',
                                'iso_three_abbreviation': 'KOR',
                                'iso_two_abbreviation': 'KR'}, name=93)
    pd.testing.assert_series_equal(example_row_2, expected_row_2)


def test_abbreviate_wikipedia_country_df():
    wikipedia_country_df = (ScrapeCountryNamesFromWikipedia().data_output()
                            .pipe(clean_wikipedia_country_df)
                            .pipe(abbreviate_wikipedia_country_df))

    example_row = wikipedia_country_df.iloc[94]
    expected_row = pd.Series({'state_name_de': 'Kosovo',
                              'full_state_name_de': 'Republik Kosovo',
                              'translation_state_name': 'Kosovo',
                              'iso_three_abbreviation': 'XXK',
                              'iso_two_abbreviation': 'XK',
                              'custom_abbreviation_state_name_de': None,
                              'custom_abbreviation_full_state_name_de': 'RK',
                              'custom_abbreviation_translation_state_name': None}, name=94,)
    pd.testing.assert_series_equal(example_row, expected_row)
