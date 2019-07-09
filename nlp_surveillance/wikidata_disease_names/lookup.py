import pandas as pd


def merge_disease_lookup_as_dict(disease_lookup: pd.DataFrame, rki_abbreviations: pd.DataFrame) -> dict:
    """Merges Wikidata German and English disease names with RKI abbreviations

    Args:
        disease_lookup:
        rki_abbreviations:

    Returns:
        Dictionary to translate German and abbreviated disease names to full English names
    """
    lookup_with_abbreviations = _merge(disease_lookup, rki_abbreviations)
    as_dict = _to_translation_dict(lookup_with_abbreviations)
    return as_dict


def _merge(disease_lookup, rki_abbreviations):
    disease_lookup_with_abbreviations = (pd.merge(disease_lookup,
                                                  rki_abbreviations,
                                                  how='outer',
                                                  on='itemLabel_DE')
                                         .applymap(lambda x: None if pd.isna(x) else x))
    return disease_lookup_with_abbreviations


def _to_translation_dict(disease_lookup):
    de_to_en = list(zip(disease_lookup.itemLabel_DE,
                        disease_lookup.itemLabel_EN))
    # Necessary so that during lookup valid English disease names are preserved
    en_to_en = list(zip(disease_lookup.itemLabel_EN,
                        disease_lookup.itemLabel_EN))
    abbreviation_to_en = list(zip(disease_lookup.abbreviation,
                                  disease_lookup.itemLabel_EN))
    de_to_en.extend(en_to_en)
    abbreviation_to_en.extend(de_to_en)
    translation_dict = dict(abbreviation_to_en)
    translation_dict = {k: v for k, v in translation_dict.items() if None not in [k, v]}
    return translation_dict
