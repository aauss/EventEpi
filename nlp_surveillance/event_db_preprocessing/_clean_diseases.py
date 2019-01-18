import utils


def clean_diseases(event_db):
    event_db.loc[:, 'disease_edb'] = event_db.loc[:, 'disease_edb'].str.strip()
    event_db = utils.split_strings_at_comma_and_distribute_to_new_rows(event_db, 'disease_edb')
    return event_db
