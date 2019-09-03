from eventepi import my_utils


def clean_diseases(event_db):
    """Remove formatting errors in disease names of incident database

    Splits single rows with several disease names to several rows
    with a single country entry and where every other entry of the
    row is duplicated form the former row.

    Args:
        event_db (pd.DataFrame): Incident database without formatted disease names

    Returns (pd.DataFrame): Incident database with cleaned disease names

    """
    event_db = my_utils.split_strings_at_comma_and_distribute_to_new_rows(event_db, 'disease_edb')
    event_db.loc[:, 'disease_edb'] = event_db.loc[:, 'disease_edb'].str.strip()
    return event_db
