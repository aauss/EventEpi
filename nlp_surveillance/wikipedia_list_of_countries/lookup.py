import re
import pandas as pd

from utils import my_utils


def to_translation_dict(country_lookup_df):
    # Important to use the sorted columns, so that the custom abbreviations are put into the dict first and are
    # overwritten by other input that has more priority
    columns = sorted(country_lookup_df.columns)
    lists_of_translation_tuples = [list(zip(country_lookup_df[other_column],
                                            country_lookup_df['translation_state_name']))
                                   for other_column in columns]
    flattened = my_utils.flatten_list(lists_of_translation_tuples)
    country_lookup_dict = dict(flattened)
    country_lookup_without_nones = {k: v for k, v in country_lookup_dict.items() if None not in [k, v]}
    return country_lookup_without_nones


def abbreviate_wikipedia_country_df(wikipedia_country_df):
    """Search for names that might have abbreviations. If they consist of two or more words that start with a capital
    letter, it makes an abbreviation out of it
    """
    columns_to_abbreviate = ["state_name_de", "full_state_name_de", "translation_state_name"]
    list_of_abbreviated_columns_as_lists = [list(map(_abbreviate_country, wikipedia_country_df[column].tolist()))
                                            for column in columns_to_abbreviate]
    merged_columns_as_list_of_rows = [list(row) for row in zip(*list_of_abbreviated_columns_as_lists)]
    custom_abbreviations = pd.DataFrame(merged_columns_as_list_of_rows,
                                        columns=['custom_abbreviation_state_name_de',
                                                 'custom_abbreviation_full_state_name_de',
                                                 'custom_abbreviation_translation_state_name'])
    wikipedia_country_df_with_custom_abbreviations = pd.concat([wikipedia_country_df, custom_abbreviations], axis=1)
    return wikipedia_country_df_with_custom_abbreviations


def _abbreviate_country(country_name):
    """Abbreviates entries of list of country names
    Example: United Kingdom --> UK
    """
    abbreviation = None
    country_name = str(country_name)
    if len(re.findall(r"([A-Z|ÄÖÜ])", country_name)) > 1:
        abbreviation = "".join(re.findall(r"([A-Z|ÄÖÜ])", country_name))
    return abbreviation
