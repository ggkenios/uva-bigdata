from src.entrypoints import ingest, bronze, silver, gold


if __name__ == "__main__":
    for module in [ingest, bronze, silver, gold]:
        module.main()
        print(f"Module '{module.__name__}' executed successfully")
