
import json
from pathlib import Path
from collections import Counter
from kafka import KafkaConsumer

# ==========================================
# 1. CONFIGURATION
# ==========================================

KAFKA_SERVER = "kafka:9092"
TOPIC_NAME = "github_events"

REPORT_DIR = Path("/app/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_PATH = REPORT_DIR / "consumer_report.json"

# ==========================================
# 2. INITIALISATION DU CONSOMMATEUR
# ==========================================

print("Connexion au topic Kafka...")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_SERVER],
    group_id="github-events-verification-2026",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=15000,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
    key_deserializer=lambda key: (
        key.decode("utf-8") if key else None
    )
)

print("Connexion réussie !")
print("Topic :", TOPIC_NAME)

# ==========================================
# 3. LECTURE DES EVENEMENTS
# ==========================================

total = 0
event_types = Counter()
errors = []
event_ids = set()
duplicates = 0
examples = []

print("\nDébut de la lecture des messages...\n")

try:
    for message in consumer:

        total += 1
        event = message.value

        event_id = str(event.get("id", ""))

        if not event_id:
            errors.append("Événement sans identifiant")

        if message.key != event_id:
            errors.append(
                f"Clé Kafka incorrecte pour l'événement {event_id}"
            )

        if event_id in event_ids:
            duplicates += 1
        else:
            event_ids.add(event_id)

        event_types[event.get("type", "Unknown")] += 1

        if len(examples) < 3:
            examples.append({
                "id": event_id,
                "type": event.get("type"),
                "repo": event.get("repo", {}).get("name")
            })

        if total % 1000 == 0:
            print(f"Messages reçus : {total}")

finally:
    consumer.close()

# ==========================================
# 4. AFFICHAGE DES RESULTATS
# ==========================================

print("\n========== RAPPORT CONSOMMATEUR ==========")

print("Messages reçus :", total)
print("Identifiants uniques :", len(event_ids))
print("Doublons :", duplicates)
print("Erreurs :", len(errors))

print("\nRépartition par type :")

for event_type, count in event_types.most_common():
    print(f"{event_type}: {count}")

print("\nExemples de messages :")

for example in examples:
    print(example)

# ==========================================
# 5. GENERATION DU RAPPORT JSON
# ==========================================

report = {
    "kafka_server": KAFKA_SERVER,
    "topic": TOPIC_NAME,
    "messages_received": total,
    "unique_event_ids": len(event_ids),
    "duplicates": duplicates,
    "errors_count": len(errors),
    "errors": errors[:20],
    "event_types": dict(event_types),
    "examples": examples
}

with open(REPORT_PATH, "w", encoding="utf-8") as output:
    json.dump(report, output, indent=4, ensure_ascii=False)

print("\nRapport enregistré :", REPORT_PATH)
print("==========================================")
