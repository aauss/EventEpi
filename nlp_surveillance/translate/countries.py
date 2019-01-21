from didyoumean import didyoumean


def translate(event_db, country_lookup):
    event_db.country_edb = (event_db.country_edb[event_db.country_edb.notna()]
                            .apply(lambda x: _translate_country(x, country_lookup)))
    return event_db


def _translate_country(country, country_lookup):
    if country in country_lookup:
        translation = country_lookup[country]
    else:
        translation = _try_to_correct_name(country, country_lookup)
    return translation


def _try_to_correct_name(country, country_lookup):
    try:
        did_u_mean = didyoumean.didYouMean(country, country_lookup.keys())
        country = country_lookup[did_u_mean]
    except KeyError:
        pass
    try:
        or_did_u_mean = _complete_partial_words(country, country_lookup)
        country = country_lookup[or_did_u_mean]
    except KeyError:
        pass
    return country


def _complete_partial_words(to_complete, country_lookup):
    match = [country for country in country_lookup.keys()
             if country.lower().startswith(to_complete.lower())
             or to_complete in country]
    if match:
        to_complete = match[0]
    return to_complete
