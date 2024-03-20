# GCP
PROJECT_ID = "uva-mba-bigdata"
CLOUD_STORAGE = "uva-bigdata-gkenios"
SERVICE_PRINCIPAL_JSON = "sp-uva-mba-bigdata.json"

# Data
DATASET = [
    {
        "name": "unemployment",
        "url": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/LFSA_URGAED/1.0?compress=false&format=csvdata&formatVersion=2.0&returnLastUpdateDate=true",
        "file_type": "csv",
    },
    {
        "name": "crime",
        "url": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/CRIM_OFF_CAT/1.0?compress=false&format=csvdata&formatVersion=2.0&returnLastUpdateDate=true",
        "file_type": "csv",
    },
    {
        "name": "pay_gap",
        "url": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/earn_gr_gpgr2/1.0?compress=false&format=csvdata&formatVersion=2.0&returnLastUpdateDate=true",
        "file_type": "csv",
    },
]
