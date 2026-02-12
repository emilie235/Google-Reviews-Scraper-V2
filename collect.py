import pandas as pd
import yaml
import subprocess
from pathlib import Path
import re
import unicodedata
import json
import shutil
from datetime import datetime

# -----------------------------
# Directories
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent
CONFIG_TEMPLATE = BASE_DIR / "config.yaml"
CONFIGS_DIR = BASE_DIR / "configs"
DATA_DIR = BASE_DIR / "data"

CONFIGS_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

# -----------------------------
# Load CSV
# -----------------------------
df = pd.read_csv("paris_restaurants_google_reviews.csv", sep=",", encoding="utf-8")
print(df.head())

# -----------------------------
# Helper: make slug
# -----------------------------
def make_slug(text):
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")

# -----------------------------
# Helper: save partial JSON
# -----------------------------
def save_json_partial(slug, docs):
    """Sauvegarde JSON dans data/<slug>/<slug>.json, backup simple"""
    folder = DATA_DIR / slug
    folder.mkdir(parents=True, exist_ok=True)

    json_file = folder / f"{slug}.json"

    # Si le fichier existe déjà, on le renomme temporairement pour backup
    if json_file.exists():
        backup_file = folder / f"{slug}.json.bak"
        shutil.move(json_file, backup_file)
        print(f"[INFO] Backup simple créé : {backup_file}")

    # Écriture du nouveau JSON
    with json_file.open("w", encoding="utf-8") as f:
        json.dump(list(docs.values()), f, ensure_ascii=False, indent=2)

    print(f"[INFO] {len(docs)} reviews sauvegardées dans {json_file}")

# -----------------------------
# Main scraping loop
# -----------------------------
failed_restaurants = []
collected_restaurants = []

try:
    for _, row in df.iterrows():
        restaurant = row["name"].strip()
        place_id = row["id"].strip()
        slug = make_slug(restaurant)

        resto_dir = DATA_DIR / slug
        resto_dir.mkdir(parents=True, exist_ok=True)

        # Load YAML template
        with open(CONFIG_TEMPLATE, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Modify config
        config["restaurant"] = restaurant
        config["url"] = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en&gl=US"
        config["json_path"] = str(resto_dir / f"{slug}.json")
        config["seen_ids_path"] = str(resto_dir / f"{slug}.ids")

        # Save YAML file
        yaml_path = CONFIGS_DIR / f"{slug}.yaml"
        with open(yaml_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, sort_keys=False, allow_unicode=True)

        # Run scraper
        python_exe = "python"
        result = subprocess.run(
            [str(python_exe), str(BASE_DIR / "start.py"), "--config", str(yaml_path)],
            text=True,
            check=True
        )
        print(f"Successfully scraped {restaurant} !")
        collected_restaurants.append(slug)

except KeyboardInterrupt:
    print("\n[INFO] Scraping interrupted by the user !")
    print(f"[INFO] Already collected restaurants : {collected_restaurants}")

    # -----------------------------
    # Sauvegarde JSON pour tout ce qui a été collecté
    # -----------------------------
    for slug in collected_restaurants:
        # Tenter de lire le JSON existant produit par start.py
        resto_json = DATA_DIR / slug / f"{slug}.json"
        if resto_json.exists():
            try:
                docs = json.loads(resto_json.read_text(encoding="utf-8"))
                docs_dict = {d["review_id"]: d for d in docs if "review_id" in d}
            except Exception:
                docs_dict = {}
        else:
            docs_dict = {}
        save_json_partial(slug, docs_dict)

finally:
    print("[INFO] End of the scraping")