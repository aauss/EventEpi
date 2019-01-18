import re


def clean_wikipedia_countries(wiki_df):
    wiki_df_without_brackets = wiki_df.applymap(lambda x: re.sub(r"\(.*\)", " ", str(x)))
    wiki_df_without_footnotes = wiki_df_without_brackets.applymap(lambda x: re.sub(r'\[\d*\]', "", x))
    wiki_df_without_dash = wiki_df_without_footnotes.applymap(_replace_wiki_dash_with_none)
    return wiki_df_without_dash


def _replace_wiki_dash_with_none(string):
    dash = u"\u2014"  # Used dash for missing entry in Wikipedia table
    if dash == string:
        return None
    else:
        return string
