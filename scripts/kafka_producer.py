
import gzip
import json

from pathlib import Path
from collections import Counter
from kafka import KafkaProducer

# ==========================================
# CONFIGURATION
# ==========================================

DATE = "2015-01-01"

DATA_DIR = Path("/app/data/raw")

KAFKA_SERVER = "kafka:9092"
TOPIC_NAME = "github_events"

REPORT_DIR = Path("/app/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_PATH = REPORT_DIR / "ingestion_report.json"

# Taille maximale : 10 Mo
MAX_REQUEST_SIZE = 10 * 1024 * 1024


# ==========================================
# RECUPERATION DES ARCHIVES
# ==========================================

def get_archive_files():

    files = [
        DATA_DIR / f"{DATE}-{hour}.json.gz"
        for hour in range(23)
    ]

    missing = [
        str(file)
        for file in files
        if not file.exists()
    ]

    if missing:
        raise FileNotFoundError(
            "Archives manquantes :\n" + "\n".join(missing)
        )

    return files


# ==========================================
# CREATION DU PRODUCTEUR KAFKA
# ==========================================

def create_producer():

    return KafkaProducer(

        bootstrap_servers=[KAFKA_SERVER],

        value_serializer=lambda event: json.dumps(
            event,
            ensure_ascii=False
        ).encode("utf-8"),

        key_serializer=lambda key: key.encode("utf-8"),

        acks="all",

        retries=10,

        max_in_flight_requests_per_connection=1,

        request_timeout_ms=40000,

        retry_backoff_ms=1000,

        max_request_size=MAX_REQUEST_SIZE
    )


# ==========================================
# INGESTION DES EVENEMENTS
# ==========================================

def main():

    files = get_archive_files()[:1]

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

            with gzip.open(
                file_path,
                "rt",
                encoding="utf-8"
            ) as file:

                for line_number, line in enumerate(file, start=1):

                    total += 1
                    file_total += 1

                    try:

                        event = json.loads(line)

                        event_id = str(event["id"])

                        producer.send(
                            TOPIC_NAME,
                            key=event_id,
                            value=event
                        ).get(timeout=60)

                        sent += 1
                        file_sent += 1

                        event_types[event["type"]] += 1

                    except Exception as error:

                        failed += 1
                        file_failed += 1

                        if len(errors) < 100:

                            errors.append({
                                "file": file_path.name,
                                "line": line_number,
                                "error": str(error)
                            })

                    if total % 1000 == 0:

                        print(
                            f"Traités : {total} | "
                            f"Envoyés : {sent} | "
                            f"Échecs : {failed}"
                        )

            producer.flush()

            files_report[file_path.name] = {
                "total": file_total,
                "sent": file_sent,
                "failed": file_failed
            }

            print(
                f"Résultat {file_path.name} : "
                f"{file_sent} envoyés, "
                f"{file_failed} échecs"
            )

    finally:

        producer.flush()
        producer.close()

    # ==========================================
    # GENERATION DU RAPPORT
    # ==========================================

    report = {

        "date": DATE,

        "source": "23 archives GH Archive",

        "kafka_server": KAFKA_SERVER,

        "topic": TOPIC_NAME,

        "archives_processed": len(files),

        "total_events": total,

        "messages_sent": sent,

        "messages_failed": failed,

        "event_types": dict(event_types),

        "files": files_report,

        "errors": errors
    }

    with open(
        REPORT_PATH,
        "w",
        encoding="utf-8"
    ) as output:

        json.dump(
            report,
            output,
            indent=4,
            ensure_ascii=False
        )

    print("\n========== RAPPORT GLOBAL D'INGESTION ==========")

    print("Archives traitées :", len(files))

    print("Total événements :", total)

    print("Messages envoyés :", sent)

    print("Messages échoués :", failed)

    print("Rapport enregistré :", REPORT_PATH)


# ==========================================
# EXECUTION
# ==========================================

if __name__ == "__main__":
    main()
