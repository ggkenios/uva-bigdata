FILTER = {
    "unemployment": [],
    "crime": [
        "iccs == 'ICCS03012'", # The code for sexual asault
        "unit == 'P_HTHAB'", # Per 100k inhabitants (rate instead of flat number)
    ],
    "pay_gap": [
        "nace_r2 == 'B-S_X_O'", # Aggreagated sectors
    ],
}

DROP_COLS = {
    "unemployment": [
        "STRUCTURE",
        "STRUCTURE_ID",
        "freq", # Only one unique value ("A")
        "unit", # Only one unique value ("PC")
        #"age",
        "OBS_FLAG",
        "LAST UPDATE",
    ],
    "crime": [
        "STRUCTURE",
        "STRUCTURE_ID",
        "freq", # Only one unique value ("A")
        "unit", # Only one unique value ("PC")
        "iccs", # Drop after filtering
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
    "crime": {
        "geo": "country",
        "TIME_PERIOD": "year",
        "OBS_VALUE": "crime_rate_per_100k",
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
        "right": "crime",
        "on": ["country", "year"],
        "how": "inner",
    },
    {
        "left": "unemployment",
        "right": "pay_gap",
        "on": ["country", "year"],
        "how": "left",
    },
]
