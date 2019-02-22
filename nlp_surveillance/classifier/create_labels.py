import pandas as pd
from collections import namedtuple


def create_labels(entities, to_optimize, event_db_entry):

    if to_optimize == 'counts':
        labels = [event_db_entry == count for count in entities]
    elif to_optimize == 'dates':
        labels = _label_dates(entities, event_db_entry)
    else:
        raise ValueError
    return labels


def _label_dates(entities, event_db_entry):
    Date = namedtuple('range', ['from_', 'to_', 'event_db_entry'])
    dates = [Date(pd.to_datetime(from_, errors='coerce'),
                  pd.to_datetime(to_, errors='coerce'),
                  event_db_entry)
             for from_, to_ in entities]
    dates_filtered = _filter_too_broad_annotated_time_spans(dates)
    labels = _assign_date_label(dates_filtered)
    return labels


def _filter_too_broad_annotated_time_spans(dates, allowed_margin=pd.Timedelta('7days')):
    return [date for date in dates if date.to_ - date.from_ <= pd.Timedelta(allowed_margin)]


def _assign_date_label(dates, allowed_margin=pd.Timedelta('3days')):
    labels = [is_in_date_range(date, allowed_margin) for date in dates]
    return labels


def is_in_date_range(date, allowed_margin):
    is_in_time_range = (((date.from_ - pd.Timedelta(allowed_margin)) <= date.event_db_entry)
                        & ((date.to_ + pd.Timedelta(allowed_margin)) >= date.event_db_entry))
    return is_in_time_range
