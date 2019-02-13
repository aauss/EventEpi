import pandas as pd


def to_datetime(event_db):
    event_db.date_of_data = event_db.date_of_data.apply(lambda x:
                                                        pd.to_datetime(x,
                                                                       dayfirst=True,
                                                                       errors='coerce'))
    return event_db
