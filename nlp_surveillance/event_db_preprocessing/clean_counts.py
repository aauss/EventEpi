import re


def clean_counts(event_db):
    """Clean count columns of incident database to remove format errors

    Args:
        event_db (pd.DataFrame): Incident database without cleaned country names

    Returns:
        Incident database with cleaned country names

    """
    event_db.count_edb = event_db["count_edb"].str.replace(',', '').str.replace('.', '').str.replace(' ', '')
    event_db.count_edb = event_db["count_edb"].apply(_keep_only_integers)
    return event_db


def _keep_only_integers(string_with_int):
    if isinstance(string_with_int, str):
        int_as_string = re.search(r'(\d)+', string_with_int)[0]
        return int(int_as_string)
    else:
        return string_with_int
