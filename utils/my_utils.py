import pickle
import pandas as pd
import numpy as np
from operator import itemgetter
from itertools import groupby


def delete_non_epitator_name_entity_tiers(anno_doc):
    del anno_doc.tiers["spacy.nes"]
    del anno_doc.tiers["spacy.noun_chunks"]
    del anno_doc.tiers["spacy.sentences"]
    del anno_doc.tiers["spacy.tokens"]
    try:
        del anno_doc.tiers["nes"]
        del anno_doc.tiers["ngrams"]
        del anno_doc.tiers["tokens"]
    except KeyError:
        pass
    return anno_doc


def return_most_occuring_string_in_list(list_of_strings):
    list_of_country_occurence_tuple = [(key, len(list(group))) for key, group in groupby(sorted(list_of_strings))]
    sorted_by_occurence = sorted(list_of_country_occurence_tuple, key=lambda x: x[1], reverse=True)
    try:
        most_occuring_string = max(sorted_by_occurence, key=itemgetter(1))[0]
    except ValueError:
        most_occuring_string = None
    return most_occuring_string


def flatten_list(to_flatten):
    return [item for sublist in to_flatten for item in sublist]


def load_rki_header_and_proxy_dict():
    # TODO: when proxy is needed again, store this pickle in data/rki
    return pickle.load(open('scraping_params.p', 'rb'))


def split_strings_at_comma_and_distribute_to_new_rows(df, split_column, split_by=','):
    list_of_splits_as_series = _split_by_comma(df, split_column, split_by)
    len_of_splits = _get_length_of_split(list_of_splits_as_series)
    repeated_entries_dict = _repeat_entries_as_dict(df, len_of_splits, split_column)
    repeated_entries_dict[split_column] = np.concatenate(list_of_splits_as_series.values)
    split_df = pd.DataFrame(repeated_entries_dict, columns=repeated_entries_dict.keys())
    split_df.loc[:, split_column] = split_df.loc[:, split_column].str.strip()
    return split_df


def split_list_and_distribute_to_new_rows(df, split_column):
    repeated_entries_dict = _repeat_entries_as_dict(df, df[split_column].apply(len), split_column)

    # np.concatenate does not work with tuples
    repeated_entries_dict[split_column] = flatten_list(df[split_column].values)
    split_df = pd.DataFrame(repeated_entries_dict, columns=repeated_entries_dict.keys())
    return split_df


def _split_by_comma(df, column, split_by):
    split = df[column].str.split(split_by)

    # Nones in column cause non-list entries that need to be transformed to list
    split = pd.Series([entry if isinstance(entry, list) else [entry]
                       for entry in split])
    return split


def _get_length_of_split(split):
    length = split.str.len()
    length = [1 if np.isnan(i) else int(i) for i in length]  # Necessary because there are Nones in column
    return length


def _repeat_entries_as_dict(df, length, split_column):
    repeated_entries_as_dict = {column: np.repeat(df[column].values, length) for column in df.columns
                                if column != split_column}
    return repeated_entries_as_dict
