import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

from eventepi.idb import IDB


def test_process_dates():
    df = pd.DataFrame(
        {
            "Datenstand für Fallzahlen gesamt*": [
                "02.03.2018",
                "43329",
                "Mitte Sept.",
                "9/27/2018",
                "09.10.2019",
                "13.10.218",
                "September 2018",
                "11/6/2018",
                "2018",
                "Jan-4.Nov 2018",
                "19.11.20108",
                "14.11.2018",
            ]
        }
    )
    df_expected = pd.DataFrame(
        {
            "date_cases_idb": [
                pd.to_datetime("02.03.2018", dayfirst=True),
                pd.NaT,
                pd.NaT,
                pd.to_datetime("27.09.2018", dayfirst=True),
                pd.to_datetime("09.10.2018", dayfirst=True),
                pd.to_datetime("13.10.2018", dayfirst=True),
                pd.to_datetime("01.09.2018", dayfirst=True),
                pd.to_datetime("06.11.2018", dayfirst=True),
                pd.to_datetime("01.01.2018", dayfirst=True),
                pd.NaT,
                pd.to_datetime("19.11.2018", dayfirst=True),
                pd.to_datetime("14.11.2018", dayfirst=True),
            ],
        }
    )
    df_processed = IDB()._process_dates(df)
    assert_series_equal(df_expected["date_cases_idb"], df_processed["date_cases_idb"])


def test_process_counts():
    df = pd.DataFrame(
        {
            "Fälle gesamt*": [
                "mind 18",
                "0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden0 bei Menschen.\nMehr als 50 Todesfälle bei Kühem. Schafen und Pferden",
                "13 430",
                "1,207,596",
                "650,000",
                "erhöhte Fallzahlen seit 2013",
                "145 ",
                "57",
            ]
        }
    )
    df_expected = pd.DataFrame(
        {
            "case_counts_idb": [
                18.0,
                0.0,
                13430.0,
                1207596.0,
                650000.0,
                np.nan,
                145.0,
                57.0,
            ]
        }
    )
    df_processed = IDB()._process_counts(df)
    assert_series_equal(df_expected["case_counts_idb"], df_processed["case_counts_idb"])


def test_process_diseases():
    df = pd.DataFrame(
        {
            "Krankheitsbild(er)": [
                "MERS",
                "Listeriose",
                "Pest",
                "Cholera",
                "Leptospirose",
                "Tularämie ",
                "Tick-borne relapsing fever ",
                "Denguefieber",
                "Anthrax",
                "Hämolytisch-urämisches Syndrom",
                "Lassafieber",
                "Lassafieber ",
                "Malaria",
                "Affenpocken",
                "Krim-Kongo Hämorrhagisches Fieber",
                "Gelbfieber",
                "Japanische-Enzephalitis",
                "Rifttalfieber",
                "Masern",
                "Kuhpocken",
                "Polio",
                "Brucellose",
                "Acute encephalitis syndrome",
                "Primary Amebic Meningo-encephalitis [PAM]",
                "Ebolafieber",
                "TBE",
                "Legionellose",
                "Hämorrhagisches Fieber",
                "AFP",
                "Influenza",
                "FSME ",
                "Nierenversagen; v.a. HUS, Leptospirose",
                "West-Nil-Fieber",
                "Poliomyelitis",
                "Husten, Fieber",
                "Bilharziose",
                "Fieber",
                "Röteln",
                "Melioidosis",
                "Buruli-Ulkus",
                "Hepatitis A",
                "Sepsis",
                "Meningitis",
                "NEC (Nekrotisierende Enterokolitis)",
                "Tollwut",
                "Mumps",
                "Hepatitis E",
                "Ebola",
                "Guillain-Barré Syndrom",
                "Lungenentzündung",
                "Norovirus-Gastroenteritis",
            ]
        }
    )
    df_expected = pd.DataFrame(
        {
            "disease_idb": [
                "Middle east respiratory syndrome",
                "Listeriosis",
                "Plague",
                "Cholera",
                "Leptospirosis",
                "Tularaemia",
                "Tick-borne relapsing fever",
                "Dengue hemorrhagic fever",
                "Anthrax",
                "Haemolytic-uremic syndrome",
                "Lassa fever",
                "Lassa fever",
                "Malaria",
                "Monkeypox",
                "Crimean-congo hemorrhagic fever",
                "Yellow fever",
                "Japanese encephalitis",
                "Rift fever",
                "Measles",
                "Cowpox",
                "Polio",
                "Brucellosis",
                "Acute encephalitis syndrome",
                "Primary amebic meningo-encephalitis",
                "Ebola fever",
                "Tick-borne encephalitis",
                "Legionellosis",
                "Hemorrhagic fever",
                "Acute flaccid paralysis",
                "Influenza",
                "Tick-borne encephalitis",
                "Leptospirosis",
                "West nile fever",
                "Poliomyelitis",
                np.nan,
                "Schistosomiasis",
                np.nan,
                "Rubella",
                "Melioidosis",
                "Buruli ulcer",
                "Hepatitis a",
                "Sepsis",
                "Meningitis",
                "Necrotising enterocolitis",
                "Rabies",
                "Mumps",
                "Hepatitis e",
                "Ebola",
                "Acute inflammatory demyelinating polyneuropathy",
                "Pneumonia",
                "Norovirus gastroenteritis",
            ]
        }
    )
    df_processed = IDB()._process_diseases(df)
    assert_series_equal(df_expected["disease_idb"], df_processed["disease_idb"])


def test_process_countries():
    df = pd.DataFrame(
        {
            "Ausgangs- bzw. Ausbruchsland": [
                "Oman ",
                "Australien",
                "Bolivien",
                "Yemen",
                "La Reunion",
                "Schweiz",
                "Israel",
                "Uganda",
                "Kenia",
                "Frankreich",
                "Liberia",
                "Nigeria",
                "Costa Rica",
                "Kamerun",
                "Indien",
                "Iran",
                "Saudi-Arabien",
                "Taiwan",
                "Vereinigte Arabische Emirate",
                "Brasilien",
                "Pakistan",
                "USA",
                "UK",
                "Irak",
                "Demokratische Republik Kongo",
                "Algerien",
                "Peru ",
                "DRC",
                "Tschechien",
                "Spanien",
                "Angola",
                "China",
                "Papua-Neuguinea",
                "Vereinigtes Königreich",
                "Kongo",
                "Französiche Guyana",
                "Kroatien",
                "Afghanistan",
                "VAE Dubai",
                "Myanmar",
                "Haiti",
                "Kuwait",
                "VAE ",
                "Romänien",
                "Deutschland",
                "Tschechische Republik",
                "Sudan",
                "Großbritannien",
                "Japan",
                "Südafrika",
                "Venezuela ",
                "Kenya",
                "Neuseeland",
                "Marokko",
                "Ireland",
                "Namibia",
                "Hong Kong",
                "Libanon",
                "Dominikanische Republik",
                "Argentinien",
                "Usbekistan",
                "Trinidad & Tobago",
                "Peru",
                "Italien",
            ]
        }
    )
    df_expected = pd.DataFrame(
        {
            "country_idb": [
                "Oman",
                "Australia",
                "Bolivia",
                "Yemen",
                "La Reunion",
                "Switzerland",
                "Israel",
                "Uganda",
                "Kenya",
                "France",
                "Liberia",
                "Nigeria",
                "Costa Rica",
                "Cameroon",
                "India",
                "Iran",
                "Saudi Arabia",
                "Taiwan",
                "United Arab Emirates",
                "Brazil",
                "Pakistan",
                "United States of America",
                "United Kingdom",
                "Iraq",
                "Democratic Republic of Congo",
                "Algeria",
                "Peru",
                "Democratic Republic of Congo",
                "Czech Republic",
                "Spain",
                "Angola",
                "China",
                "Papua New Guinea",
                "United Kingdom",
                "Congo",
                "French Guyana",
                "Croatia",
                "Afghanistan",
                "United Arab Emirates",
                "Myanmar",
                "Haiti",
                "Kuwait",
                "United Arab Emirates",
                "Romania",
                "Germany",
                "Czech Repubic",
                "Sudan",
                "United Kingdom",
                "Japan",
                "South Africa",
                "Venezuela",
                "Kenya",
                "New Zealand",
                "Morocco",
                "Ireland",
                "Namibia",
                "Hong Kong",
                "Lebanon",
                "Dominican Republic",
                "Argentina",
                "Uzbekistan",
                "Trinidad and Tobago",
                "Peru",
                "Italy",
            ]
        }
    )
    df_processed = IDB()._process_countries(df)
    assert_series_equal(df_expected["country_idb"], df_processed["country_idb"])
