import os
import pandas as pd


def get_rki_abbreviations() -> pd.DataFrame:
    """Load RKI internal disease abbreviations

    Returns:
        Disease names and abbreviations
    """
    dirname = os.path.dirname(__file__)
    path = os.path.join(dirname, '..', '..', 'data', 'rki', 'disease_codes.csv')
    disease_code_df = pd.read_csv(path, ';')
    disease_code_df = disease_code_df[['Code', 'TypeName']]
    disease_code_df = disease_code_df.rename(columns={'TypeName': 'itemLabel_DE',
                                                      'Code': 'abbreviation'})  # Rename for later merging
    return disease_code_df
