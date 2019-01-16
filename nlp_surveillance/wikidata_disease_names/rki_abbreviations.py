import os
import pandas as pd


def _get_codes():
    dirname = os.path.dirname(__file__)
    path = os.path.join(dirname, '..', '..', 'rki', 'disease_code.csv')
    disease_code_df = pd.read_csv(path, ';')
    disease_code_df = disease_code_df[['Code', 'TypeName']]
    return disease_code_df
