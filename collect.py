import pandas as pd
import yaml
import subprocess
from pathlib import Path

import re
import unicodedata

# Creating directories

BASE_DIR = Path(__file__).resolve().parent
CONFIG_TEMPLATE = BASE_DIR / "config.yaml"
CONFIGS_DIR = BASE_DIR / "configs"
DATA_DIR = BASE_DIR / "data"

CONFIGS_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

# Loading dataframe

df = pd.read_csv("C:/Users/leemi/OneDrive/Documents/ENSAE/RA/Google reviews scraping/paris_restaurants_google_reviews.csv", sep=",", encoding="utf-8")
print(df.head())

# Normalizing restaurant's names to create slugs

def make_slug(text):
    # Supprime les accents
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    # Minuscules
    text = text.lower()
    # Remplace tout caractère non alphanumérique par _
    text = re.sub(r"[^a-z0-9]+", "_", text)
    # Supprime les _ en début et fin
    return text.strip("_")

# Iterating over dataframe rows and generating config files

failed_restaurants = []

for _, row in df.iterrows():
    restaurant = row["name"].strip()
    id = row["id"].strip() 
    slug = make_slug(restaurant)

    # create a folder for each restaurant's data
    resto_dir = DATA_DIR / slug
    resto_dir.mkdir(parents=True, exist_ok=True)

    # loading yaml file template
    with open(CONFIG_TEMPLATE, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # modifying config values 

    config["restaurant"] = restaurant
    config["url"] = (
        f"https://www.google.com/maps/place/?q=place_id:{id}"
    )
    config["json_path"] = str(resto_dir / f"{slug}.json")
    config["seen_ids_path"] = str(resto_dir / f"{slug}.ids")

    # saving YAML file
    yaml_path = CONFIGS_DIR / f"{slug}.yaml"
    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, sort_keys=False, allow_unicode=True)

    # Running the scraper with the generated config file

    try:
        python_exe = BASE_DIR / "venv" / "Scripts" / "python.exe"
        result = subprocess.run(
            [str(python_exe), str(BASE_DIR / "start.py"), "--config", str(yaml_path)],
            text=True,
            check=True
        )
        print(f"Successfully scraped {restaurant} !")
        if result.stdout:
            print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error for: {restaurant}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        failed_restaurants.append(restaurant)