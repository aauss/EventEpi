from deep_eq import deep_eq
from edb_clean import *
from my_utils import flatten_list


# print("Start testing Ereignisdatenbank cleaning.")
# print("Testing cleaning country names...")
def test_country_cleaning():
    example_countries_to_clean = pd.Series([" Australien",
                                            "Kongo \nUSA", "Italien, Deutschland, Belgien ", "Franz._Polynesien",
                                            "Trinidad & Tobago"])
    expected_countries_to_clean = ['Trinidad und Tobago', 'Franz. Polynesien', ['USA', 'Kongo'], 'Australien',
                                   ['Italien', 'Deutschland', 'Belgien']]
    assert deep_eq(sorted(flatten_list(example_countries_to_clean.apply(clean_country_name).tolist())),
                   sorted(flatten_list(expected_countries_to_clean))), "Cleaning country names failed"
    # print("...testing cleaning country names completed.")

    # print("Testing country name translation...")


def test_translate_abbreviations():
    example_to_abbreviate = ["USA", "VAE", 'Italien', "DR Cong", ["Deutschland", "EU"], ["Belgien", "DRC"]]
    desired_output = ['Vereinigte Staaten', 'Vereinigte Arabische Emirate', 'Italien', 'DR Cong',
                      ['Deutschland', 'Europäische Union'],
                      ['Belgien', 'Kongo, Demokratische Republik']]

    example = flatten_list([translate_abbreviation(country) for country in example_to_abbreviate])
    assert deep_eq(flatten_list(example), flatten_list(desired_output))
    #     print("Test successful")
    # else:
    #     print("Test failed")
    # print("...testing country name translation completed")
    #
    # print("Testing Ereignisdatenbank cleaning completed.")


def test_translation():
    example_to_translate = ["Deutschland", "Delaware", ["Kongo", "China"], "Niger"]
    expected_result_translate = [ 'Germany',
                                 'Delaware',
                                 [['Congo, Democratic Republic of the (Kinshasa)',
                                    'Congo, Republic of (Brazzaville)'],
                                  ['Taiwan oder Republic of China', 'China']],
                                 'Niger']
    assert deep_eq(flatten_list([translate(example) for example in example_to_translate]),
                   flatten_list(expected_result_translate))


def test_edb_to_timestamp():
    example_to_timestamp = ['28.06.2018\nSechs Kühe auf einem Bauernhof gestorben.', '23.6.2018 1. Todesfall',
                          'Mai 2018', '08.09.2017', '32.12.2017']
    expected_result_to_timestamp = ["2018-06-28", '2018-06-23', 'Mai 2018', '2017-09-08', '32.12.2017']
    assert deep_eq([edb_to_timestamp(time) for time in example_to_timestamp], expected_result_to_timestamp)
