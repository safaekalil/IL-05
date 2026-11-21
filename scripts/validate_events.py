
import gzip
import json
from pathlib import Path
from collections import Counter
from datetime import datetime

# ==========================================
# 1. CONFIGURATION
# ==========================================

file_path = Path("/app/data/raw/2015-01-01-15.json.gz")

report_dir = Path("/app/reports")
report_dir.mkdir(parents=True, exist_ok=True)

# Champs obligatoires
required_fields = [
    "id",
    "type",
    "actor",
    "repo",
    "created_at"
]

# ==========================================
# 2. INITIALISATION
# ==========================================

total = 0
valid = 0
invalid = 0
duplicates = 0

seen_ids = set()
event_types = Counter()
errors = []

# ==========================================
# 3. VERIFICATION DU FICHIER
# ==========================================

if not file_path.exists():
    print("Erreur : fichier introuvable")
    exit()

print("Début de la validation GH Archive...\n")

# ==========================================
# 4. LECTURE ET VALIDATION
# ==========================================

with gzip.open(file_path, "rt", encoding="utf-8") as file:

    for line_number, line in enumerate(file, start=1):

        total += 1

        try:

            event = json.loads(line)

            if not isinstance(event, dict):
                raise ValueError("L'événement n'est pas un objet JSON")

            missing = [
                field for field in required_fields
                if field not in event
            ]

            if missing:
                raise ValueError(
                    f"Champs manquants : {missing}"
                )

            if not isinstance(event["actor"], dict):
                raise ValueError("actor invalide")

            if not event["actor"].get("login"):
                raise ValueError("Login du développeur manquant")

            if not isinstance(event["repo"], dict):
                raise ValueError("repo invalide")

            if not event["repo"].get("name"):
                raise ValueError("Nom du dépôt manquant")

            # Vérification de la date
            datetime.fromisoformat(
                event["created_at"].replace("Z", "+00:00")
            )

            # Détection des doublons
            event_id = str(event["id"])

            if event_id in seen_ids:
                duplicates += 1
            else:
                seen_ids.add(event_id)

            valid += 1
            event_types[event["type"]] += 1

        except (json.JSONDecodeError, ValueError, TypeError) as error:

            invalid += 1

            if len(errors) < 10:
                errors.append({
                    "line": line_number,
                    "error": str(error)
                })

# ==========================================
# 5. AFFICHAGE DES RESULTATS
# ==========================================

print("========== RAPPORT DE VALIDATION ==========")

print("Total des événements :", total)
print("Événements valides   :", valid)
print("Événements invalides :", invalid)
print("Doublons détectés    :", duplicates)

print("\nRépartition par type :")

for event_type, count in event_types.most_common():
    print(f"{event_type}: {count}")

# ==========================================
# 6. GENERATION DU RAPPORT JSON
# ==========================================

report = {
    "source": file_path.name,
    "total_events": total,
    "valid_events": valid,
    "invalid_events": invalid,
    "duplicate_events": duplicates,
    "event_types": dict(event_types),
    "errors": errors
}

report_path = report_dir / "validation_report.json"

with open(report_path, "w", encoding="utf-8") as output:

    json.dump(
        report,
        output,
        indent=4,
        ensure_ascii=False
    )

print("\nRapport enregistré :", report_path)
print("===========================================")
