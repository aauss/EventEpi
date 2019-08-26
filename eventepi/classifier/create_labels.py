import pandas as pd

from collections import namedtuple
from typing import List, Any


def create_labels(entities: list, to_optimize: str, event_db_entry: Any) -> List[bool]:
    """Takes a list of entities and a target entity from the incident database

    Args:
        entities: List of found entities
        to_optimize: Entity type (e.g. dates or counts)
        event_db_entry: Target entity. This is the entity (date or count) found in the incident database

    Returns:
        A list of boolean weather inserted entity matches the target entity of the incident database
    """
    if to_optimize == 'counts':
        labels = [int(event_db_entry) == count for count in entities]
    elif to_optimize == 'dates':
        labels = _label_dates(entities, event_db_entry)
    else:
        raise ValueError
    return labels


def _label_dates(entities: list, event_db_entry: namedtuple):
    Date = namedtuple('range', ['from_', 'to_', 'event_db_entry'])
    dates = [Date(pd.to_datetime(from_, errors='coerce'),
                  pd.to_datetime(to_, errors='coerce'),
                  event_db_entry)
             for from_, to_ in entities]
    dates_filtered = _filter_too_broad_annotated_time_spans(dates)
    labels = _assign_date_label(dates_filtered)
    return labels


def _filter_too_broad_annotated_time_spans(dates: List[namedtuple], allowed_margin=pd.Timedelta('7days')):
    return [date for date in dates if date.to_ - date.from_ <= pd.Timedelta(allowed_margin)]


def _assign_date_label(dates: List[namedtuple], allowed_margin=pd.Timedelta('3days')):
    labels = [is_in_date_range(date, allowed_margin) for date in dates]
    return labels


def is_in_date_range(date: namedtuple, allowed_margin: pd.Timedelta):
    is_in_time_range = (((date.from_ - pd.Timedelta(allowed_margin)) <= date.event_db_entry)
                        & ((date.to_ + pd.Timedelta(allowed_margin)) >= date.event_db_entry))
    return is_in_time_range
