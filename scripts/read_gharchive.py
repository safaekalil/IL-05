
import gzip
import json
from pathlib import Path

# ==========================================
# 1. CONFIGURATION
# ==========================================

file_path = Path("/app/data/raw/2015-01-01-15.json.gz")

# ==========================================
# 2. VERIFICATION DU FICHIER
# ==========================================

if not file_path.exists():
    print("Erreur : fichier introuvable")
    exit()

print("Fichier trouvé !")

# ==========================================
# 3. LECTURE DES EVENEMENTS JSON
# ==========================================

events = []

with gzip.open(file_path, "rt", encoding="utf-8") as file:

    for line in file:
        event = json.loads(line)
        events.append(event)

# ==========================================
# 4. AFFICHAGE DES RESULTATS
# ==========================================

print("\nNombre total d'evenements :", len(events))

print("\nLES 5 PREMIERS EVENEMENTS :\n")

for event in events[:5]:

    print(json.dumps(
        event,
        indent=4,
        ensure_ascii=False
    ))

    print("-" * 60)
