FILTER = {
    "unemployment": [
        "age == 'Y15-64'", # The common age range value for both dataframes
    ],
    "education": [
        "age == 'Y15-64'", # The common age range value for both dataframes
    ],
    "pay_gap": [
        "nace_r2 == 'B-S_X_O'",
    ],
}

DROP_COLS = {
    "unemployment": [
        "STRUCTURE",
        "STRUCTURE_ID",
        "freq", # Only one unique value ("A")
        "unit", # Only one unique value ("PC")
        "age", # Drop after filtering
        "OBS_FLAG",
        "LAST UPDATE",
    ],
    "education": [
        "STRUCTURE",
        "STRUCTURE_ID",
        "freq", # Only one unique value ("A")
        "unit", # Only one unique value ("PC")
        "age", # Drop after filtering
        "OBS_FLAG",
        "LAST UPDATE",
    ],
    "pay_gap": [
        "STRUCTURE",
        "STRUCTURE_ID",
        "freq", # Only one unique value ("A")
        "unit", # Only one unique value ("PC")
        "nace_r2", # Drop after filtering
        "OBS_FLAG",
        "LAST UPDATE",
    ],
}

RENAME_COLS = {
    "unemployment": {
        "geo": "country",
        "TIME_PERIOD": "year",
        "OBS_VALUE": "unemployment_rate",
    },
    "education": {
        "geo": "country",
        "TIME_PERIOD": "year",
        "OBS_VALUE": "population_rate",
    },
    "pay_gap": {
        "geo": "country",
        "TIME_PERIOD": "year",
        "OBS_VALUE": "pay_gap_rate",
    },
}  

JOINS = [
    {
        "left": "unemployment",
        "right": "education",
        "on": ["sex", "isced11", "country", "year"],
        "how": "left",
    },
    {
        "left": "unemployment",
        "right": "pay_gap",
        "on": ["country", "year"],
        "how": "left",
    },
]
