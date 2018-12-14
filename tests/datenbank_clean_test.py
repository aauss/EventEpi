from nlp_surveillance.deep_eq import deep_eq
from nlp_surveillance.edb_clean import *
from nlp_surveillance.edb_clean import _translate_abbreviation
from nlp_surveillance.my_utils import flatten_list


def test_country_cleaning():
    example_countries_to_clean = pd.Series([" Australien",
                                            "Kongo \nUSA", "Italien, Deutschland, Belgien ", "Franz._Polynesien",
                                            "Trinidad & Tobago"])
    expected_countries_to_clean = ['Trinidad und Tobago', 'Franz. Polynesien', ['USA', 'Kongo'], 'Australien',
                                   ['Italien', 'Deutschland', 'Belgien']]
    assert deep_eq(sorted(flatten_list(example_countries_to_clean.apply(clean_country_name).tolist())),
                   sorted(flatten_list(expected_countries_to_clean))), "Cleaning country names failed"


def test_translate_abbreviations():
    example_to_abbreviate = ["USA", "VAE", 'Italien', "DR Cong", ["Deutschland", "EU"], ["Belgien", "DRC"]]
    desired_output = ['Vereinigte Staaten', 'Vereinigte Arabische Emirate', 'Italien', 'DR Cong',
                      ['Deutschland', 'Europäische Union'],
                      ['Belgien', 'Demokratische Republik Kongo']]

    example = flatten_list([_translate_abbreviation(country) for country in example_to_abbreviate])
    assert deep_eq(flatten_list(example), flatten_list(desired_output))


def test_geoname_translation():
    example_to_translate = ["Deutschland", "Delaware", ["Kongo", "China"], "Niger"]
    expected_result_translate = ['Germany', 'Delaware',
                                 [['Democratic Republic of the Congo', 'Republic of Congo'],
                                  ['Taiwan oder Republic of China', 'China']], 'Niger']
    assert deep_eq(flatten_list([translate_geonames(example) for example in example_to_translate]),
                   flatten_list(expected_result_translate))


def test_edb_to_timestamp():
    example_to_timestamp = ['28.06.2018\nSechs Kühe auf einem Bauernhof gestorben.', '23.6.2018 1. Todesfall',
                            'Mai 2018', '08.09.2017', '32.12.2017']
    expected_result_to_timestamp = ["2018-06-28", '2018-06-23', 'Mai 2018', '2017-09-08', '32.12.2017']
    assert deep_eq([edb_to_timestamp(time) for time in example_to_timestamp], expected_result_to_timestamp)


def test_translate_disease_name():
    assert translate_disease_name('Warzee, Gonorrhoee, BPS') == ['wart', 'gonorrhea', 'pertussis']
    assert translate_disease_name('Ebolda') == 'Ebolda'
    assert translate_disease_name("Ebola") == 'Ebola virus disease'
