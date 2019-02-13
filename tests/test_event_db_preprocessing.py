import pandas as pd

from nlp_surveillance.event_db_preprocessing.clean_countries import clean_countries
from nlp_surveillance.event_db_preprocessing.clean_counts import clean_counts
from nlp_surveillance.event_db_preprocessing.clean_dates import to_datetime
from nlp_surveillance.event_db_preprocessing.clean_diseases import clean_diseases
from nlp_surveillance.event_db_preprocessing.clean_urls import clean_urls


def test_clean_countries():
    example_countries_to_clean = pd.DataFrame({'country_edb': [' Australien',
                                                               'Kongo \nUSA', 'Italien, Deutschland, Belgien ',
                                                               'Franz._Polynesien', 'Trinidad & Tobago']})
    expected_clean_countries = pd.DataFrame({'country_edb': ['Australien', 'Kongo', 'USA',
                                                             'Italien', 'Deutschland', 'Belgien',
                                                             'Franz. Polynesien', 'Trinidad und Tobago']})
    cleaned_countries = clean_countries(example_countries_to_clean)
    pd.testing.assert_frame_equal(cleaned_countries, expected_clean_countries)


def test_clean_counts():
    example_counts_to_clean = pd.DataFrame({'count_edb': ['1,078,997',
                                                          '40 abzgl. 19 non-cases',
                                                          'mind 18',
                                                          '4 cVDPV2\n2 WPV1 in Afghanistan', '>1000',
                                                          '13 430',
                                                          '446.150']})
    expected_clean_counts = pd.DataFrame({'count_edb': [1078997, 40, 18, 4, 1000, 13430, 446150]})
    cleaned_counts = clean_counts(example_counts_to_clean)
    pd.testing.assert_frame_equal(cleaned_counts, expected_clean_counts)


def test_to_datetime():
    example_to_datetime = pd.DataFrame({'date_of_data': ['12.01.2018',
                                              '01.12.2018',
                                              '12/09/18',
                                              '32.12.2018',
                                              '25.8.18',
                                              '13 May 2017',
                                              '01.1.218']})
    expected_to_datetime = pd.DataFrame({'date_of_data': [pd.Timestamp('2018-01-12 00:00:00'),
                                                          pd.Timestamp('2018-12-01 00:00:00'),
                                                          pd.Timestamp('2018-09-12 00:00:00'),
                                                          pd.NaT,
                                                          pd.Timestamp('2018-08-25 00:00:00'),
                                                          pd.Timestamp('2017-05-13 00:00:00'),
                                                          pd.NaT]})
    dates_to_datetime = to_datetime(example_to_datetime)
    pd.testing.assert_frame_equal(dates_to_datetime, expected_to_datetime)


def test_clean_diseases():
    example_diseases_to_clean = pd.DataFrame({'disease_edb': ['Diarrhoe , Überkeit, Erbrechen',
                                                              ' Lassafieber',
                                                              'Ebola ']})
    expected_clean_diseases = pd.DataFrame({'disease_edb': ['Diarrhoe',
                                                                      'Überkeit',
                                                                      'Erbrechen',
                                                                      'Lassafieber',
                                                                      'Ebola']})
    cleaned_diseases = clean_diseases(example_diseases_to_clean)
    pd.testing.assert_frame_equal(cleaned_diseases, expected_clean_diseases)


def test_clean_urls():
    example_urls_to_clean = pd.DataFrame({'URL_1': ['<http://apps.who.int/iris/10665/1/OEW10.pdf>',
                                                    ' http://www.promedmail.org/post/4 '
                                                    ', http://www.promedmail.org/post/5 ',
                                                    'mail'],
                                          'URL_2': ['http://www.google.com', None, None]})

    expected_clean_urls = pd.DataFrame({'URL': ['http://apps.who.int/iris/10665/1/OEW10.pdf',
                                                'http://www.google.com',
                                                'https://www.promedmail.org/post/4',
                                                'https://www.promedmail.org/post/5',
                                                None]})
    cleaned_urls = clean_urls(example_urls_to_clean)
    pd.testing.assert_frame_equal(cleaned_urls, expected_clean_urls)
