from didyoumean import didyoumean
from tqdm import tqdm


def translate(event_db, disease_lookup):
    """Translates disease names in incident database using the disease_lookup

    disease_lookup contains translations from different forms of abbreviations and German disease names to
    a controlled English vocabulary that matches EpiTator output
    Args:
        event_db (pd.DataFrame): The incident database
        disease_lookup (dict): Dictionary to translate German disease names to controlled vocabulary

    Returns (pd.DataFrame):
        The incident database where all disease names transferred to a controlled vocabulary

    """

    tqdm.pandas()
    event_db["disease_edb"] = (event_db["disease_edb"][event_db["disease_edb"].notna()]
                               .progress_apply(lambda x: _translate_disease(x, disease_lookup)))
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
        or_did_u_mean = _complete_partial_words(disease, disease_lookup)
        disease = disease_lookup[or_did_u_mean]
    except KeyError:
        pass
    return disease


def _complete_partial_words(to_complete, disease_lookup):
    # Takes a list of incomplete disease names and tries to complete them: Ebola -> Ebolavirus
    match = [disease for disease in disease_lookup.keys() if disease.lower().startswith(to_complete.lower())]
    if match:
        to_complete = match[0]
    return to_complete
