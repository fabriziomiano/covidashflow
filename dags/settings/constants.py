"""
Settings Module
"""
import datetime as dt
from collections import OrderedDict

from .vars import (
    NEW_POSITIVE_KEY,
    TOTAL_CASES_KEY,
    VARS,
    VAX_BOOSTER_DOSE_KEY,
    VAX_FIRST_DOSE_KEY,
    VAX_SECOND_DOSE_KEY,
)

CUM_QUANTITIES = [q for q in VARS if VARS[q]["type"] == "cum"]
NON_CUM_QUANTITIES = [q for q in VARS if VARS[q]["type"] == "current"]
DAILY_QUANTITIES = [
    quantity
    for quantity in VARS
    if VARS[quantity]["type"] == "daily" and quantity.endswith("_ma")
]
TREND_CARDS = [
    quantity
    for quantity in VARS
    if not quantity.endswith("_ma") and VARS[quantity]["type"] != "vax"
]
PROV_TREND_CARDS = [TOTAL_CASES_KEY, NEW_POSITIVE_KEY]
VAX_DOSES = [VAX_FIRST_DOSE_KEY, VAX_SECOND_DOSE_KEY, VAX_BOOSTER_DOSE_KEY]

LOCKDOWN_DAY = dt.datetime(2020, 3, 22)
PHASE2_DAY = dt.datetime(2020, 5, 4)
PHASE3_DAY = dt.datetime(2020, 6, 15)
CRITICAL_AREAS_DAY = dt.datetime(2020, 11, 6)
VACCINE_DAY = dt.datetime(2020, 12, 27)
SERIES_DT_FMT = "d MMM yy"
DOW_FMTY = "EEE d MMM"
KEY_PERIODS = OrderedDict()

KEY_PERIODS["lockdown"] = {
    "title": "Lockdown",
    "text": "Lockdown",
    "color": "red",
    "from": LOCKDOWN_DAY,
    "to": PHASE2_DAY,
    "n_days": (PHASE2_DAY - LOCKDOWN_DAY).days,
}
KEY_PERIODS["phase2"] = {
    "title": "Phase 2",
    "text": '"Phase 2"',
    "color": "orange",
    "from": PHASE2_DAY,
    "to": PHASE3_DAY,
    "n_days": (PHASE3_DAY - PHASE2_DAY).days,
}
KEY_PERIODS["phase3"] = {
    "title": "Phase 3",
    "text": '"Phase 3"',
    "color": "green",
    "from": PHASE3_DAY,
    "to": CRITICAL_AREAS_DAY,
    "n_days": (CRITICAL_AREAS_DAY - PHASE3_DAY).days,
}
KEY_PERIODS["critical_areas"] = {
    "title": "Critical Areas",
    "text": "Critical areas",
    "color": "red",
    "from": CRITICAL_AREAS_DAY,
    "to": dt.datetime.today(),
    "n_days": (dt.datetime.today() - CRITICAL_AREAS_DAY).days,
}
KEY_PERIODS["vaccine_day"] = {
    "title": "Vaccine day",
    "text": "Vaccine day",
    "color": "blue",
    "from": VACCINE_DAY,
    "to": dt.datetime.today(),
    "n_days": (dt.datetime.today() - VACCINE_DAY).days,
}
ITALY_MAP = {
    "Abruzzo": ["Chieti", "L'Aquila", "Pescara", "Teramo"],
    "Basilicata": ["Matera", "Potenza"],
    "Calabria": [
        "Catanzaro",
        "Cosenza",
        "Crotone",
        "Reggio di Calabria",
        "Vibo Valentia",
    ],
    "Campania": ["Avellino", "Benevento", "Caserta", "Napoli", "Salerno"],
    "Emilia-Romagna": [
        "Bologna",
        "Ferrara",
        "Forlì-Cesena",
        "Modena",
        "Parma",
        "Piacenza",
        "Ravenna",
        "Reggio nell'Emilia",
        "Rimini",
    ],
    "Friuli Venezia Giulia": ["Gorizia", "Pordenone", "Trieste", "Udine"],
    "Lazio": ["Frosinone", "Latina", "Rieti", "Roma", "Viterbo"],
    "Liguria": ["Genova", "Imperia", "La Spezia", "Savona"],
    "Lombardia": [
        "Bergamo",
        "Brescia",
        "Como",
        "Cremona",
        "Lecco",
        "Lodi",
        "Mantova",
        "Milano",
        "Monza e della Brianza",
        "Pavia",
        "Sondrio",
        "Varese",
    ],
    "Marche": ["Ancona", "Ascoli Piceno", "Fermo", "Macerata", "Pesaro e Urbino"],
    "Molise": ["Campobasso", "Isernia"],
    "Piemonte": [
        "Alessandria",
        "Asti",
        "Biella",
        "Cuneo",
        "Novara",
        "Torino",
        "Verbano-Cusio-Ossola",
        "Vercelli",
    ],
    "Puglia": [
        "Bari",
        "Barletta-Andria-Trani",
        "Brindisi",
        "Lecce",
        "Foggia",
        "Taranto",
    ],
    "Sardegna": ["Cagliari", "Nuoro", "Sassari", "Sud Sardegna"],
    "Sicilia": [
        "Agrigento",
        "Caltanissetta",
        "Catania",
        "Enna",
        "Messina",
        "Palermo",
        "Ragusa",
        "Siracusa",
        "Trapani",
    ],
    "Toscana": [
        "Arezzo",
        "Firenze",
        "Grosseto",
        "Livorno",
        "Lucca",
        "Massa Carrara",
        "Pisa",
        "Pistoia",
        "Prato",
        "Siena",
    ],
    "P.A. Bolzano": [],
    "P.A. Trento": [],
    "Umbria": ["Perugia", "Terni"],
    "Valle d'Aosta": ["Aosta"],
    "Veneto": [
        "Belluno",
        "Padova",
        "Rovigo",
        "Treviso",
        "Venezia",
        "Verona",
        "Vicenza",
    ],
}
REGIONS = [key for key in ITALY_MAP]
PROVINCES = [p for pp in ITALY_MAP.values() for p in pp]
PC_TO_OD_MAP = {
    "Italia": "ITA",
    "Abruzzo": "ABR",
    "Basilicata": "BAS",
    "Calabria": "CAL",
    "Campania": "CAM",
    "Emilia-Romagna": "EMR",
    "Friuli Venezia Giulia": "FVG",
    "Lazio": "LAZ",
    "Liguria": "LIG",
    "Lombardia": "LOM",
    "Marche": "MAR",
    "Molise": "MOL",
    "P.A. Bolzano": "PAB",
    "P.A. Trento": "PAT",
    "Piemonte": "PIE",
    "Puglia": "PUG",
    "Sardegna": "SAR",
    "Sicilia": "SIC",
    "Toscana": "TOS",
    "Umbria": "UMB",
    "Valle d'Aosta": "VDA",
    "Veneto": "VEN",
}
OD_TO_PC_MAP = {
    "ITA": "Italia",
    "ABR": "Abruzzo",
    "BAS": "Basilicata",
    "CAL": "Calabria",
    "CAM": "Campania",
    "EMR": "Emilia-Romagna",
    "FVG": "Friuli Venezia Giulia",
    "LAZ": "Lazio",
    "LIG": "Liguria",
    "LOM": "Lombardia",
    "MAR": "Marche",
    "MOL": "Molise",
    "PAB": "P.A. Bolzano",
    "PAT": "P.A. Trento",
    "PIE": "Piemonte",
    "PUG": "Puglia",
    "SAR": "Sardegna",
    "SIC": "Sicilia",
    "TOS": "Toscana",
    "UMB": "Umbria",
    "VDA": "Valle d'Aosta",
    "VEN": "Veneto",
}
TRANSLATION_DIRNAME = "translations"
DEFAULT_DAG_ARGS = {
    "owner": "COVIDash",
    "depends_on_past": True,
    "start_date": dt.datetime(2022, 3, 21),
    "email": ["fabriziomiano@gmail.com"],
    "email_on_failure": True,
    "schedule_interval": "*5 * * * *",
}
