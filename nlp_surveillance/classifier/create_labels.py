import pandas as pd


def create_labels(event_db, to_optimize):
    if to_optimize == 'counts':
        event_db['label'] = event_db.count_edb == event_db.counts
    elif to_optimize == 'dates':
        event_db[['from', 'to']] = event_db['dates'].apply(pd.Series)
        event_db[['from', 'to']] = event_db[['from', 'to']].applymap(lambda x: pd.to_datetime(x, errors='coerce'))
        event_db = _filter_too_broad_annotated_time_spans(event_db)
        event_db = _assign_date_label(event_db)
    else:
        raise NotImplementedError
    return event_db[['label', 'sentence']]


def _filter_too_broad_annotated_time_spans(event_db, allowed_margin=pd.Timedelta('7days')):
    return event_db[event_db['to'] - event_db['from'] <= pd.Timedelta(allowed_margin)]


def _assign_date_label(event_db, allowed_margin=pd.Timedelta('3days')):
    is_in_time_range = (((event_db['from'] - pd.Timedelta(allowed_margin)) <= event_db['date_of_data'])
                        & ((event_db['to'] + pd.Timedelta(allowed_margin)) >= event_db['date_of_data']))

    event_db = event_db.assign(label=is_in_time_range)
    return event_db
