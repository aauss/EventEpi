import re
import pandas as pd


def abbreviate_wikipedia_country_df(wikipedia_country_df):
    """Search for names that might have abbreviations. If they consist of two or more words that start with a capital
    letter, it makes an abbreviation out of it
    """

    columns_to_abbreviate = ["state_name_de", "full_state_name_de", "translation_state_name"]
    abbreviated_columns = [list(map(_abbreviate_country, wikipedia_country_df[column].tolist()))
                           for column in columns_to_abbreviate]
    merged_columns = [list(a) for a in zip(*abbreviated_columns)]
    custom_abbreviations = pd.DataFrame(merged_columns,
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


