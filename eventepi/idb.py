from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from translate import Translator
from tqdm import tqdm


@dataclass
class IDB:
    """Reads and preprocesses the IDB"""
    idb_path: pd.DataFrame = (Path(__file__).parent.resolve() / Path("../data/idb.csv"))
    idb_processed_path: pd.DataFrame = (
        Path(__file__).parent.resolve() / Path("../data/idb_processed.csv")
    )
    df: pd.DataFrame = pd.read_csv(idb_path)
    df_processed: pd.DataFrame = pd.DataFrame()
    translator = Translator(from_lang="de", to_lang="en", email="a.abbood94@gmail.com")
    tqdm.pandas(desc="Translate German content to English")

    def preprocess(self):
        """Preprocesses the IDB"""
        self.df_processed = (
            self.df.replace(["nan", "-", "", " ", "?", "keine"], [np.nan] * 6)
            .assign(
                date_cases_idb=lambda x: pd.to_datetime(
                    x["Datenstand für Fallzahlen gesamt*"],
                    dayfirst=True,
                    errors="coerce",
                )
            )
            .dropna(how="all")
            .melt(
                id_vars=[
                    "Ausgangs- bzw. Ausbruchsland",
                    "Krankheitsbild(er)",
                    "date_cases_idb",
                    "case_counts_idb",
                ],
                value_vars=[
                    "Link zur Quelle 1",
                    "Link zur Quelle 2",
                    "Link zur Quelle 3",
                    "Link zur Quelle 4",
                ],
                var_name="url_type_idb",
                value_name="url_idb",
            )
            .pipe(self._process_counts)
            .pipe(self._process_urls)
            .pipe(self._process_countries)
            .pipe(self._process_diseases)
            .drop(columns=["Ausgangs- bzw. Ausbruchsland", "Krankheitsbild(er)"])
        )
        self.df_processed = self.df_processed[
            ~self.df_processed.duplicated(
                [
                    "country_idb",
                    "disease_idb",
                    "case_counts_idb",
                    "date_cases_idb",
                    "url_idb",
                ]
            )
        ]

    def _process_urls(self, df):
        return (
            df.pipe(self._keep_dons_and_promed_urls)
            .pipe(self._filter_invalid_urls)
            .pipe(self._normalize_promed_urls)
        )

    def _process_countries(self, df):
        return df.assign(
            country_idb=lambda x: x["Ausgangs- bzw. Ausbruchsland"]
            .str.strip()
            .str.replace("&", "und")
            .replace(
                ["USA", "UK", "Großbritannien", "DRC", "VAE", "VAE Dubai", "Kenya"],
                [
                    "Vereinigte Staaten von Amerika",
                    "Vereinigtes Köngireich",
                    "Vereinigtes Köngireich",
                    "Demokratische Republik Kongo",
                    "Vereinigte Arabische Emirate",
                    "Vereinigte Arabische Emirate",
                    "Kenia",
                ],
            )
            .progress_apply(
                lambda x: self.translator.translate(x) if x is not np.nan else x
            )
        )

    def _process_counts(self, df):
        return df.assign(
            case_counts_idb=lambda x: x["Fälle gesamt*"]
            .str.strip()
            .str.replace(",", "")
            .str.replace(".", "")
            .str.replace(" ", "")
            .str.extract(r"(\d+)", expand=False)
            .astype(float)
        )

    def _process_diseases(self, df):
        return df.assign(
            disease_idb=lambda x: x["Krankheitsbild(er)"]
            .str.strip()
            .replace(
                [
                    "MERS",
                    "Denguefieber",
                    "Japanische-Enzephalitis",
                    "NEC (Nekrotisierende Enterokolitis)",
                    "Nierenversagen; v.a. HUS, Leptospirose",
                    "FSME",
                    "TBE",
                    "AFP",
                    "Fieber",
                    "Husten, Fieber",
                ],
                [
                    "Middle East respiratory syndrome",
                    "dengue hemorrhagic fever",
                    "Japanese encephalitis",
                    "Nekrotisierende Enterokolitis",
                    "Leptospirose",
                    "Frühsommer-Meningoenzephalitis",
                    "Tick-borne encephalitis",
                    "acute flaccid paralysis",
                    np.nan,
                    np.nan,
                ],
            )
            .str.replace("\[PAM\]", "")
            .progress_apply(
                lambda x: self.translator.translate(x) if x is not np.nan else x
            )
            .replace(["BURULI ULCER", "ET NANBH"], ["Buruli ulcer", "Hepatitis E"])
        )

    def _keep_dons_and_promed_urls(self, df):
        df = df[df["url_idb"].str.contains("/don|promed", na=False)]
        return df.loc[df["url_idb"] != "http://www.promedmail.org/"]

    def _filter_invalid_urls(self, df):
        return df[df["url_idb"].astype(str).str.contains("http")]

    def _normalize_promed_urls(self, df):
        df.loc[
            df["url_idb"].str.contains("promed"), "url_idb"
        ] = "https://promedmail.org/promed-post/?id=&id=" + df.loc[
            df["url_idb"].str.contains("promed"), "url_idb"
        ].str.extract(
            r"[/\.](\d{7})", expand=False
        )  # Extract ProMED ID and use it to rewrite all URLs uniformly
        return df.replace('https://www.promedmail.org/ Active number 20180428.5771404', "https://promedmail.org/promed-post/?id=&id=5771404")
