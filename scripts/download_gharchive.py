
import argparse
import gzip
import os
import requests

from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_DATE = "2015-01-01"
DEFAULT_HOUR = 15

DEFAULT_OUTPUT_DIR = os.getenv(
    "DATA_RAW_DIR",
    "/app/data/raw"
)


def create_session():

    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)

    return session


def verify_gzip(file_path):

    try:
        with gzip.open(file_path, "rb") as file:

            while file.read(1024 * 1024):
                pass

        return True

    except (OSError, EOFError):
        return False


def download_archive(date, hour, output_dir):

    filename = f"{date}-{hour}.json.gz"

    url = f"https://data.gharchive.org/{filename}"

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / filename
    temp_path = output_dir / (filename + ".part")

    if file_path.exists():

        if verify_gzip(file_path):

            print("Archive déjà téléchargée et valide :", filename)
            return file_path

        print("Archive existante invalide. Nouveau téléchargement.")

        file_path.unlink()

    print("Téléchargement GH Archive...")
    print("URL :", url)

    try:

        with create_session() as session:

            response = session.get(
                url,
                stream=True,
                timeout=(15, 120)
            )

            response.raise_for_status()

            with open(temp_path, "wb") as output:

                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):

                    if chunk:
                        output.write(chunk)

        if not temp_path.exists() or temp_path.stat().st_size == 0:
            raise RuntimeError("Fichier téléchargé vide")

        if not verify_gzip(temp_path):
            raise RuntimeError("Archive gzip invalide")

        temp_path.replace(file_path)

        print("\nTéléchargement terminé !")
        print("Fichier :", file_path)

        print(
            "Taille :",
            round(file_path.stat().st_size / 1024, 2),
            "KB"
        )

        return file_path

    except Exception:

        if temp_path.exists():
            temp_path.unlink()

        raise


def main():

    parser = argparse.ArgumentParser(
        description="Téléchargement des archives GH Archive"
    )

    parser.add_argument("--date", default=DEFAULT_DATE)
    parser.add_argument("--hour", type=int, default=DEFAULT_HOUR)
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR
    )

    args = parser.parse_args()

    if not 0 <= args.hour <= 23:
        parser.error("L'heure doit être comprise entre 0 et 23")

    try:

        download_archive(
            args.date,
            args.hour,
            args.output_dir
        )

    except Exception as error:

        print("Erreur téléchargement :", error)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
