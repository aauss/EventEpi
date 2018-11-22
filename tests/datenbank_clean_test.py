from deep_eq import deep_eq
from datenbank_clean import *
from utils import flatten_list

print("Start testing Ereignisdatenbank cleaning.")

print("Testing cleaning country names...")
example_countries_to_clean = [" Australien",
                              "Kongo \nUSA",
                              "Italien, Deutschland, Belgien ",
                              "Franz._Polynesien",
                              "Trinidad & Tobago"]
expected_countries_to_clean = ['Trinidad und Tobago', 'Franz. Polynesien', ['USA', 'Kongo'], 'Australien',
                               ['Italien', 'Deutschland', 'Belgien']]


assert deep_eq(sorted(flatten_list(clean_country_names(example_countries_to_clean))),
               sorted(flatten_list(expected_countries_to_clean))), "Cleaning country names failed"
print("...testing cleaning country names completed.")

print("Testing Ereignisdatenbank cleaning completed.")
