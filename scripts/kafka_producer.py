
import gzip
import json
import time
from pathlib import Path
from collections import Counter
from kafka import KafkaProducer

# ==========================================
# 1. CONFIGURATION
# ==========================================

KAFKA_SERVER = "kafka:9092"
TOPIC_NAME = "github_events"

FILE_PATH = Path("/app/data/raw/2015-01-01-15.json.gz")

REPORT_DIR = Path("/app/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_PATH = REPORT_DIR / "ingestion_report.json"

# ==========================================
# 2. INITIALISATION DU PRODUCTEUR
# ==========================================

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda event: json.dumps(
            event, ensure_ascii=False
        ).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=40000,
        retry_backoff_ms=1000
    )


# ==========================================
# 3. VERIFICATION DU FICHIER
# ==========================================

if not FILE_PATH.exists():
    raise FileNotFoundError(f"Fichier introuvable : {FILE_PATH}")

# ==========================================
# 4. INITIALISATION
# ==========================================

print("Connexion à Kafka...")
producer = create_producer()
print("Connexion Kafka réussie !")

total = 0
sent = 0
failed = 0

event_types = Counter()
errors = []

# ==========================================
# 5. LECTURE ET ENVOI
# ==========================================

print("\nDébut de l'ingestion Kafka...\n")

try:
    with gzip.open(FILE_PATH, "rt", encoding="utf-8") as file:

        for line_number, line in enumerate(file, start=1):
            total += 1

            try:
                event = json.loads(line)
                event_id = str(event["id"])

                success = False

                for attempt in range(1, 4):
                    try:
                        producer.send(
                            TOPIC_NAME,
                            key=event_id,
                            value=event
                        ).get(timeout=60)

                        success = True
                        break

                    except Exception as error:
                        print(
                            f"Erreur ligne {line_number}, "
                            f"tentative {attempt}/3 : {error}"
                        )

                        producer.close()
                        time.sleep(2)
                        producer = create_producer()

                if success:
                    sent += 1
                    event_types[event["type"]] += 1
                else:
                    failed += 1
                    errors.append({
                        "line": line_number,
                        "event_id": event_id,
                        "error": "Échec après 3 tentatives"
                    })

            except Exception as error:
                failed += 1
                errors.append({
                    "line": line_number,
                    "error": str(error)
                })

            if total % 1000 == 0:
                print(
                    f"Traités : {total} | "
                    f"Envoyés : {sent} | "
                    f"Échecs : {failed}"
                )

finally:
    producer.flush()
    producer.close()

# ==========================================
# 6. RAPPORT D'INGESTION
# ==========================================

report = {
    "source": FILE_PATH.name,
    "kafka_server": KAFKA_SERVER,
    "topic": TOPIC_NAME,
    "total_events": total,
    "messages_sent": sent,
    "messages_failed": failed,
    "event_types": dict(event_types),
    "errors": errors
}

with open(REPORT_PATH, "w", encoding="utf-8") as output:
    json.dump(report, output, indent=4, ensure_ascii=False)

print("\n========== RAPPORT INGESTION ==========")
print("Total événements :", total)
print("Messages envoyés :", sent)
print("Messages échoués :", failed)
print("Rapport enregistré :", REPORT_PATH)
print("=======================================")
