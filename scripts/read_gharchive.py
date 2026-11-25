
import gzip
import json
from pathlib import Path

DATE = "2015-01-01"
DATA_DIR = Path("/app/data/raw")


def get_archive_files():
    files = [
        DATA_DIR / f"{DATE}-{hour}.json.gz"
        for hour in range(23)
    ]

    missing = [str(file) for file in files if not file.exists()]

    if missing:
        raise FileNotFoundError(
            "Archives manquantes :\n" + "\n".join(missing)
        )

    return files


def main():
    files = get_archive_files()

    total_events = 0
    examples = []

    print(f"Lecture des 23 archives GH Archive du {DATE}\n")

    for file_path in files:
        file_count = 0

        with gzip.open(file_path, "rt", encoding="utf-8") as file:
            for line in file:
                event = json.loads(line)
                file_count += 1
                total_events += 1

                if len(examples) < 5:
                    examples.append(event)

        print(f"{file_path.name} : {file_count} événements")

    print("\n========== RÉSULTAT GLOBAL ==========")
    print("Nombre d'archives :", len(files))
    print("Nombre total d'événements :", total_events)

    print("\nLES 5 PREMIERS ÉVÉNEMENTS :\n")

    for event in examples:
        print(json.dumps(event, indent=4, ensure_ascii=False))
        print("-" * 60)


if __name__ == "__main__":
    main()
