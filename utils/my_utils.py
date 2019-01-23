import pickle
import pandas as pd
import numpy as np


def flatten_list(to_flatten):
    return [item for sublist in to_flatten for item in sublist]


def load_rki_header_and_proxy_dict():
    # TODO: when proxy is needed again, store this pickle in data/rki
    return pickle.load(open('scraping_params.p', 'rb'))


def split_strings_at_comma_and_distribute_to_new_rows(df, column, split_by=','):
    split, length = _split_by_comma_and_then_calc_len_of_split(df, column, split_by)
    repeated_entries_dict = _repeat_entries_as_dict(df, length, column)
    repeated_entries_dict[column] = np.concatenate(split.values)
    split_df = pd.DataFrame(repeated_entries_dict, columns=repeated_entries_dict.keys())
    split_df.loc[:, column] = split_df.loc[:, column].str.strip()
    return split_df


def _split_by_comma_and_then_calc_len_of_split(df, column, split_by):
    split = df[column].str.split(split_by)
    # Nones in column cause non-list entries that need to be transformed to list
    split = pd.Series([entry if isinstance(entry, list) else [entry]
                       for entry in split])

    length = split.str.len()
    length = [1 if np.isnan(i) else int(i) for i in length]  # Also caused by Nones in column
    return split, length


def _repeat_entries_as_dict(df, length, split_column):
    repeated_entries_as_dict = {column: np.repeat(df[column].values, length) for column in df.columns
                                if column != split_column}
    return repeated_entries_as_dict
