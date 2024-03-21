from src.entrypoints import ingest, bronze, silver, gold


MODULES_TO_RUN = [
    ingest, 
    bronze, 
    silver, 
    gold,
]


if __name__ == "__main__":
    for module in MODULES_TO_RUN:
        module.main()
        print(f"Module '{module.__name__}' executed successfully")
