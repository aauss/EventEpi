import os
import pandas as pd


def get_RKI_abbreviations():
    # TODO: Maybe think about a cleaner function for the German disease names (Röteln, konnatal -> Röteln)
    dirname = os.path.dirname(__file__)
    path = os.path.join(dirname, '..', '..', 'data', 'rki', 'disease_codes.csv')
    disease_code_df = pd.read_csv(path, ';')
    disease_code_df = disease_code_df[['Code', 'TypeName']]
    disease_code_df = disease_code_df.rename(columns={'TypeName': 'itemLabel_DE'})  # Rename for later merging
    return disease_code_df
