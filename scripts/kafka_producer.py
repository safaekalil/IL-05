import argparse
import gzip
import json
from collections import Counter
from pathlib import Path

from kafka import KafkaProducer


DATE = "2015-01-01"
LAST_HOUR = 22
DATA_DIR = Path("/app/data/raw")

KAFKA_SERVER = "kafka:9092"
TOPIC_NAME = "github_events"
MAX_REQUEST_SIZE = 10 * 1024 * 1024

REPORT_DIR = Path("/app/reports")
REPORT_PATH = REPORT_DIR / "ingestion_report.json"


def get_archive_files(start_hour):
    files = [
        DATA_DIR / f"{DATE}-{hour}.json.gz"
        for hour in range(start_hour, LAST_HOUR + 1)
    ]

    missing = [str(path) for path in files if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "Archives manquantes :\n" + "\n".join(missing)
        )

    return files


def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda event: json.dumps(
            event, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=40_000,
        retry_backoff_ms=1_000,
        max_request_size=MAX_REQUEST_SIZE,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Publier les archives GH Archive dans Kafka."
    )
    parser.add_argument(
        "--start-hour",
        type=int,
        choices=range(LAST_HOUR + 1),
        default=0,
        help="Première heure à publier. Par défaut : 0.",
    )
    args = parser.parse_args()

    start_hour = args.start_hour
    files = get_archive_files(start_hour)

    report_path = REPORT_PATH
    if start_hour > 0:
        report_path = REPORT_DIR / (
            f"ingestion_report_hours_{start_hour}_to_{LAST_HOUR}.json"
        )
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print("Connexion à Kafka...")
    producer = create_producer()
    print("Connexion Kafka réussie !")
    print("Topic :", TOPIC_NAME)
    print("Archives à traiter :", len(files))

    total = 0
    sent = 0
    failed = 0
    event_types = Counter()
    errors = []
    files_report = {}

    try:
        for file_path in files:
            file_total = 0
            file_sent = 0
            file_failed = 0

            print(f"\nTraitement de {file_path.name}...")

            with gzip.open(file_path, "rt", encoding="utf-8") as archive:
                for line_number, line in enumerate(archive, start=1):
                    total += 1
                    file_total += 1

                    try:
                        event = json.loads(line)
                        event_id = str(event["id"])
                        event_type = event["type"]

                        producer.send(
                            TOPIC_NAME,
                            key=event_id,
                            value=event,
                        ).get(timeout=60)
                    except Exception as error:
                        failed += 1
                        file_failed += 1

                        if len(errors) < 100:
                            errors.append({
                                "file": file_path.name,
                                "line": line_number,
                                "error": str(error),
                            })
                    else:
                        sent += 1
                        file_sent += 1
                        event_types[event_type] += 1

                    if total % 1_000 == 0:
                        print(
                            f"Traités : {total} | "
                            f"Envoyés : {sent} | "
                            f"Échecs : {failed}"
                        )

            producer.flush()

            files_report[file_path.name] = {
                "total": file_total,
                "sent": file_sent,
                "failed": file_failed,
            }
            print(
                f"Résultat {file_path.name} : "
                f"{file_sent} envoyés, {file_failed} échecs"
            )
    finally:
        producer.close()

    report = {
        "date": DATE,
        "source": "GH Archive",
        "kafka_server": KAFKA_SERVER,
        "topic": TOPIC_NAME,
        "hours_processed": list(range(start_hour, LAST_HOUR + 1)),
        "archives_processed": len(files_report),
        "total_events": total,
        "messages_sent": sent,
        "messages_failed": failed,
        "event_types": dict(event_types),
        "files": files_report,
        "errors": errors,
    }

    with report_path.open("w", encoding="utf-8") as output:
        json.dump(report, output, indent=4, ensure_ascii=False)
        output.write("\n")

    print("\n========== RAPPORT GLOBAL D'INGESTION ==========")
    print("Archives traitées :", len(files_report))
    print("Total événements :", total)
    print("Messages envoyés :", sent)
    print("Messages échoués :", failed)
    print("Rapport enregistré :", report_path)

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())