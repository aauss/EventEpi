import pandas as pd
import numpy as np


def flatten_list(to_flatten):
    return [item for sublist in to_flatten for item in sublist]


def split_up_rows_at_comma_of_entires_of_column(df, column):
    split, length = _split_by_comma_and_then_calc_len_of_split(df, column)
    repeated_entries_dict = _repeat_entries_as_dict(df, length, column)
    repeated_entries_dict[column] = np.concatenate(split.values)
    split_event_db = pd.DataFrame(repeated_entries_dict, columns=repeated_entries_dict.keys())
    split_event_db.loc[:, column] = split_event_db.loc[:, column].str.strip()
    return split_event_db


def _split_by_comma_and_then_calc_len_of_split(df, column):
    split = df[column].str.split(',')
    # Nones in column cause non-list entries that need to be transformed to list
    split = pd.Series([entry if isinstance(entry, list) else [entry]
                       for entry in split])

    length = split.str.len()
    length = [1 if np.isnan(i) else int(i) for i in length]  # Also caused by Nones in column
    return split, length


def _repeat_entries_as_dict(df, length, split_column):
    columns = df.columns
    return {column: np.repeat(df[column].values, length) for column in columns
            if column != split_column}
