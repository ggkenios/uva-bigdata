from src.entrypoints import (
    ingest,
    bronze,
    silver,
    gold,
)


if __name__ == "__main__":
    ingest.main()
    bronze.main()
    silver.main()
    gold.main()
