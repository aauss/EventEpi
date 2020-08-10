import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm
from translate import Translator

sys.path.append(str((Path(__file__).parent.resolve() / Path("..")).resolve()))
from eventepi.corpus_reader import EpiCorpusReader
from eventepi.scraper import ProMedScraper


@dataclass
class IDB:
    """Reads and preprocesses the incident database (IDB)"""

    idb_path: pd.DataFrame = (Path(__file__).parent.resolve() / Path("../data/idb.csv"))
    idb_processed_path: pd.DataFrame = (
        Path(__file__).parent.resolve() / Path("../data/idb_processed.csv")
    )
    df: pd.DataFrame = pd.read_csv(idb_path)
    df_processed: pd.DataFrame = pd.DataFrame()
    translator: Translator = Translator(
        from_lang="de", to_lang="en", email="a.abbood94@gmail.com"
    )
    tqdm.pandas(desc="Translate German content to English")

    def preprocess(self, repeat_preprocessing=False) -> "IDB":
        """Preprocesses the IDB"""
        path = Path(__file__).parent.resolve() / Path("../data/idb_processed.csv")
        if path.exists() and not repeat_preprocessing:
            self.df_processed = pd.read_csv(path, encoding="utf-8")
        else:
            self.df_processed = (
                self.df.replace(["nan", "-", "", " ", "?", "keine"], [np.nan] * 6)
                .melt(
                    id_vars=[
                        "Ausgangs- bzw. Ausbruchsland",
                        "Krankheitsbild(er)",
                        "Datenstand für Fallzahlen gesamt*",
                        "Fälle gesamt*",
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
                .pipe(self._process_urls)
                .pipe(self._process_dates)
                .pipe(self._process_counts)
                .pipe(self._process_countries)
                .pipe(self._process_diseases)
                .drop(
                    columns=[
                        "Ausgangs- bzw. Ausbruchsland",
                        "Krankheitsbild(er)",
                        "Fälle gesamt*",
                        "Datenstand für Fallzahlen gesamt*",
                        "url_type_idb",
                    ]
                )
                .dropna(how="all")
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

            self.df_processed = self.df_processed.pipe(self._create_fileids_from_urls)
            self.df_processed.to_csv(
                path, index=False,
            )
        return self

    def _process_dates(self, df):
        df.loc[:, "Datenstand für Fallzahlen gesamt*"] = df[
            "Datenstand für Fallzahlen gesamt*"
        ].replace(
            ["9/27/2018", "11/6/2018", "09.10.2019", "19.11.20108", "13.10.218"],
            ["27.09.2018", "06.11.2018", "09.10.2018", "19.11.2018", "13.10.2018"],
        )
        return df.assign(
            date_cases_idb=lambda x: pd.to_datetime(
                x["Datenstand für Fallzahlen gesamt*"], dayfirst=True, errors="coerce",
            )
        )

    def _process_counts(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.replace("erhöhte Fallzahlen seit 2013", np.nan).assign(
            case_counts_idb=lambda x: x["Fälle gesamt*"]
            .str.strip()
            .str.replace(",", "")
            .str.replace(" ", "")
            .str.extract(r"(\d+)", expand=False)
            .astype(float)
        )

    def _process_urls(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.assign(url_idb=lambda x: x["url_idb"].str.strip())
            .pipe(self._keep_dons_and_promed_urls)
            .pipe(self._keep_valid_urls)
            .pipe(self._normalize_promed_urls)
        )
        return df[df["url_idb"].notna()]

    def _process_countries(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def _process_diseases(self, df: pd.DataFrame) -> pd.DataFrame:
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
                    "Dengue hemorrhagic fever",
                    "Japanese encephalitis",
                    "Nekrotisierende Enterokolitis",
                    "Leptospirose",
                    "Frühsommer-Meningoenzephalitis",
                    "Tick-borne encephalitis",
                    "Acute flaccid paralysis",
                    np.nan,
                    np.nan,
                ],
            )
            .str.replace("\[PAM\]", "")
            .progress_apply(
                lambda x: self.translator.translate(x) if x is not np.nan else x
            )
            .replace(["BURULI ULCER", "ET NANBH"], ["Buruli ulcer", "Hepatitis E"])
            .str.replace(".", "")
            .str.capitalize()
        )

    def _keep_dons_and_promed_urls(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["url_idb"].str.contains("/don|promed", na=False)]
        return df[df["url_idb"] != "http://www.promedmail.org/"]

    def _keep_valid_urls(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df["url_idb"].astype(str).str.contains("http")]

    def _normalize_promed_urls(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[
            df["url_idb"].str.contains("promed"), "url_idb"
        ] = r"https://promedmail.org/promed-post/?id=&id=" + df.loc[
            df["url_idb"].str.contains("promed"), "url_idb"
        ].str.extract(
            r"(\d{7})$", expand=False
        )  # Extract ProMED IDs and use them to rewrite all URLs uniformly
        return df.replace(
            "https://www.promedmail.org/ Active number 20180428.5771404",
            "https://promedmail.org/promed-post/?id=&id=5771404",
        )

    def _create_fileids_from_urls(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(fileid=lambda x: x["url_idb"])

        df.loc[df["fileid"].str.contains("who"), "fileid"] = df.loc[
            df["fileid"].str.contains("who"), "fileid"
        ].str.extract(r"don(/.*?)/", expand=False)

        df.loc[df["fileid"].str.contains("promed"), "fileid"] = df.loc[
            df["fileid"].str.contains("promed"), "fileid"
        ].str.extract(r"(\d{7})", expand=False)

        reader = EpiCorpusReader()
        fildeids = reader.fileids()
        df = df.assign(
            fileid=lambda x: x["fileid"].apply(
                lambda x: [fildeid for fildeid in fildeids if x in fildeid]
            )
        )

        # Rescrape missing ProMED posts
        urls_that_were_not_scraped = df.loc[
            df.fileid.apply(lambda x: len(x)) == 0, "url_idb"
        ]
        if not urls_that_were_not_scraped.empty:
            missing_ids = urls_that_were_not_scraped.str.extract(
                r"id=(\d{7})", expand=False
            )
            scraper = ProMedScraper()
            for missing_id in missing_ids:
                scraper._scrape_id(missing_id)
            return self._create_fileids_from_urls(df)
        return df.assign(fileid=lambda x: x["fileid"].apply(lambda x: x[0]))
