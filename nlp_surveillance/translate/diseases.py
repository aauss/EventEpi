from didyoumean import didyoumean


def translate(event_db, disease_lookup):
    event_db.disease_edb = (event_db.disease_edb
                            [event_db.disease_edb.notna()]
                            .apply(lambda x: _translate_disease(x, disease_lookup)))
    return event_db


def _translate_disease(disease, disease_lookup):
    if disease in disease_lookup:
        translation = disease_lookup[disease]
    else:
        translation = _try_to_correct_name(disease, disease_lookup)
    return translation


def _try_to_correct_name(disease, disease_lookup):
    try:
        did_u_mean = didyoumean.didYouMean(disease, disease_lookup.keys())
        disease = disease_lookup[did_u_mean]
    except KeyError:
        pass
    try:
        or_did_u_mean = _try_complete_partial_words(disease, disease_lookup)
        disease = disease_lookup[or_did_u_mean]
    except KeyError:
        pass
    return disease


def _try_complete_partial_words(disease, disease_lookup):
    try:
        complete_name = _complete_partial_words(disease, disease_lookup)
        translation = disease_lookup[complete_name]
        return translation
    except KeyError:
        return disease


def _complete_partial_words(to_complete, disease_lookup):
    # Takes a list of incomplete disease names and tries to complete them: Ebola -> Ebolavirus
    match = [disease for disease in disease_lookup.keys() if disease.lower().startswith(to_complete.lower())]
    if match:
        to_complete = match[0]
    return to_complete
