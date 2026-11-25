
import gzip
import json
from pathlib import Path
from collections import Counter
from datetime import datetime

DATE = "2015-01-01"
DATA_DIR = Path("/app/data/raw")

REPORT_DIR = Path("/app/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_PATH = REPORT_DIR / "validation_report.json"

REQUIRED_FIELDS = [
    "id",
    "type",
    "actor",
    "repo",
    "created_at"
]


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
    total = 0
    valid = 0
    invalid = 0
    duplicates = 0

    seen_ids = set()
    event_types = Counter()
    errors = []
    files_report = {}

    files = get_archive_files()

    print(f"Validation des 23 archives du {DATE}...\n")

    for file_path in files:
        file_total = 0
        file_valid = 0
        file_invalid = 0

        print("Validation :", file_path.name)

        with gzip.open(file_path, "rt", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                total += 1
                file_total += 1

                try:
                    event = json.loads(line)

                    if not isinstance(event, dict):
                        raise ValueError("L'événement n'est pas un objet JSON")

                    missing = [
                        field for field in REQUIRED_FIELDS
                        if field not in event
                    ]

                    if missing:
                        raise ValueError(f"Champs manquants : {missing}")

                    if not isinstance(event["actor"], dict):
                        raise ValueError("actor invalide")

                    if not event["actor"].get("login"):
                        raise ValueError("Login du développeur manquant")

                    if not isinstance(event["repo"], dict):
                        raise ValueError("repo invalide")

                    if not event["repo"].get("name"):
                        raise ValueError("Nom du dépôt manquant")

                    datetime.fromisoformat(
                        event["created_at"].replace("Z", "+00:00")
                    )

                    event_id = str(event["id"])

                    if event_id in seen_ids:
                        duplicates += 1
                    else:
                        seen_ids.add(event_id)

                    valid += 1
                    file_valid += 1
                    event_types[event["type"]] += 1

                except (json.JSONDecodeError, ValueError, TypeError) as error:
                    invalid += 1
                    file_invalid += 1

                    if len(errors) < 50:
                        errors.append({
                            "file": file_path.name,
                            "line": line_number,
                            "error": str(error)
                        })

        files_report[file_path.name] = {
            "total": file_total,
            "valid": file_valid,
            "invalid": file_invalid
        }

        print(
            f"  Total : {file_total} | "
            f"Valides : {file_valid} | "
            f"Invalides : {file_invalid}"
        )

    report = {
        "date": DATE,
        "archives_processed": len(files),
        "total_events": total,
        "valid_events": valid,
        "invalid_events": invalid,
        "duplicate_events": duplicates,
        "unique_event_ids": len(seen_ids),
        "event_types": dict(event_types),
        "files": files_report,
        "errors": errors
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as output:
        json.dump(report, output, indent=4, ensure_ascii=False)

    print("\n========== RAPPORT GLOBAL DE VALIDATION ==========")
    print("Archives traitées :", len(files))
    print("Total des événements :", total)
    print("Événements valides :", valid)
    print("Événements invalides :", invalid)
    print("Doublons :", duplicates)

    print("\nRépartition par type :")

    for event_type, count in event_types.most_common():
        print(f"{event_type} : {count}")

    print("\nRapport enregistré :", REPORT_PATH)


if __name__ == "__main__":
    main()
