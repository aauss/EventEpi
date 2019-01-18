import re
import pandas as pd


def to_translation_dict(country_lookup_df):
    columns = country_lookup_df.columns
    directions_to_translate_to = [('translation_state_name', other_column)
                                  for other_column in columns]
    lists_of_translation_tuples = [list(zip(country_lookup_df[from_lang_x], country_lookup_df[to_lang_y]))
                                   for from_lang_x, to_lang_y in directions_to_translate_to]
    flattened = [item for sublist in lists_of_translation_tuples for item in sublist]
    country_lookup_dict = dict(flattened)
    country_lookup_wihtou_nones = {k: v for k, v in country_lookup_dict.items() if None not in [k, v]}
    return country_lookup_wihtou_nones


def clean_wikipedia_countries(wikipedia_country_df):
    wiki_df_without_brackets = wikipedia_country_df.applymap(lambda x: re.sub(r"\(.*\)", " ", str(x)))
    wiki_df_without_footnotes = wiki_df_without_brackets.applymap(lambda x: re.sub(r'\[\d*\]', "", x))
    wiki_df_without_dash = wiki_df_without_footnotes.applymap(_replace_wiki_dash_with_none)
    return wiki_df_without_dash


def _replace_wiki_dash_with_none(string):
    dash = u"\u2014"  # Used dash for missing entry in Wikipedia table
    if dash == string:
        return None
    else:
        return string


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
