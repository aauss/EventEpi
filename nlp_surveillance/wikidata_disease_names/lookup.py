import pandas as pd


def merge_disease_lookup_as_dict(disease_lookup, rki_abbreviations):
    lookup_with_abbreviations = _merge(disease_lookup, rki_abbreviations)
    as_dict = _to_translation_dict(lookup_with_abbreviations)
    return as_dict


def _to_translation_dict(disease_lookup):
    de_to_en = list(zip(disease_lookup.itemLabel_DE,
                        disease_lookup.itemLabel_EN))
    en_to_en = list(zip(disease_lookup.itemLabel_EN,
                        disease_lookup.itemLabel_EN))
    abbreviation_to_en = list(zip(disease_lookup.abbreviation,
                                  disease_lookup.itemLabel_EN))
    de_to_en.extend(en_to_en)
    abbreviation_to_en.extend(de_to_en)
    translation_dict = dict(abbreviation_to_en)
    translation_dict = {k: v for k, v in translation_dict.items() if None not in [k, v]}
    return translation_dict


def _merge(disease_lookup, rki_abbreviations):
    disease_lookup_with_abbreviations = (pd.merge(disease_lookup,
                                                  rki_abbreviations,
                                                  how='outer',
                                                  on='itemLabel_DE')
                                         .applymap(lambda x: None if pd.isna(x) else x))
    return disease_lookup_with_abbreviations
