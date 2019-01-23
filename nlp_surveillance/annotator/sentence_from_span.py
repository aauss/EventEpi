def extract_from_annotated_df(event_db, to_optimize):
    event_db = _drop_unnecessary_columns_and_nans(event_db)





def _drop_unnecessary_columns_and_nans(event_db, to_optimize):
    event_db = event_db[event_db[to_optimize].notna()]
    event_db = event_db[[to_optimize, 'annotated_text', 'extracted_text']]
    return event_db
