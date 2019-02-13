import re


def clean_wikipedia_country_df(wikipedia_country_df):
    wiki_df_without_brackets_and_footnotes = wikipedia_country_df.applymap(_remove_brackets_and_footnotes)
    wiki_df_without_dash = wiki_df_without_brackets_and_footnotes.applymap(_replace_wiki_dash_with_none)
    wiki_df_with_cleaned_state_name_de = _clean_state_name_de(wiki_df_without_dash)
    wiki_df_reordered_at_comma = wiki_df_with_cleaned_state_name_de.applymap(_reorder_words_in_names_with_comma)
    wiki_df_without_redundant_spaces = wiki_df_reordered_at_comma.applymap(lambda x: re.sub(r'(\s{2,})', ' ', x))
    return wiki_df_without_redundant_spaces


def _remove_brackets_and_footnotes(string):
    without_brackets = re.sub(r"\(.*\)", "", str(string))
    without_footnotes = re.sub(r'\[\d*\]', "", without_brackets)
    return without_footnotes


def _replace_wiki_dash_with_none(string):
    dash = u"\u2014"  # Used dash for missing entry in Wikipedia table
    if dash == string:
        return None
    else:
        return string


def _clean_state_name_de(wikipedia_country_df):
    # Remove additonal information which parts of the country are currently included
    wikipedia_country_df.state_name_de = wikipedia_country_df.state_name_de.apply(lambda x:
                                                                                  re.sub(r"((mit)|(ohne)).*", "", x))
    # Remove soft hyphen used in "Zentralafr. Rep".
    wikipedia_country_df.state_name_de = wikipedia_country_df.state_name_de.str.replace("\xad", "")
    return wikipedia_country_df


def _reorder_words_in_names_with_comma(country_name):
    # Formats such that Congo, Republik of (Brazzaville) --> Republik of Congo

    country_name = str(country_name)
    if "," in country_name:
        # If there is a comma, switch order to yield a more common abbreviation: Korea, Nord --> Nord Korea
        matched = re.match(r"([A-Za-z]*), (.*)", country_name)  # Extract words
        try:
            as_first, as_second = matched[2], matched[1]
            if as_first.islower():
                as_first = as_first.capitalize()
            country_name = as_first + " " + as_second  # Patch words together in new order
        except TypeError:
            pass
    return country_name
