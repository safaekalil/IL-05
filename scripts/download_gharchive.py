
import requests
from pathlib import Path

# ==========================================
# 1. CONFIGURATION
# ==========================================

DATE = "2015-01-01"
HOUR = 15

filename = f"{DATE}-{HOUR}.json.gz"

url = f"https://data.gharchive.org/{filename}"

# ==========================================
# 2. DOSSIER DE STOCKAGE
# ==========================================

data_dir = Path("/app/data/raw")

data_dir.mkdir(parents=True, exist_ok=True)

file_path = data_dir / filename

# ==========================================
# 3. TELECHARGEMENT
# ==========================================

try:

    if file_path.exists():

        print("Le fichier existe déjà :", filename)

    else:

        print("Téléchargement GH Archive...")
        print("URL :", url)

        response = requests.get(
            url,
            stream=True,
            timeout=60
        )

        response.raise_for_status()

        temp_path = file_path.with_name(filename + ".part")

        with open(temp_path, "wb") as file:

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):

                if chunk:
                    file.write(chunk)

        temp_path.replace(file_path)

        print("Téléchargement terminé !")

    print("\nFichier :", file_path)

    print(
        "Taille :",
        round(file_path.stat().st_size / 1024, 2),
        "KB"
    )

except requests.exceptions.RequestException as error:

    print("Erreur :", error)
