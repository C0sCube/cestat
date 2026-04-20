import os
import sys
from app.utils import Helper

# setup root
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

utils = Helper()

# -----------------------
# LOAD CONFIG ONCE
# -----------------------

PATHS_JSON = os.path.join(ROOT_DIR, "paths.json")
PATHS = utils.load_json(PATHS_JSON)

# -----------------------
# BASE PATHS
# -----------------------

BASE_DIR = PATHS["root_dir"]

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
LOG_DIR = os.path.join(OUTPUT_DIR, "logs")
DATA_DIR = os.path.join(OUTPUT_DIR, "data")
NGT_DATA_DIR = os.path.join(OUTPUT_DIR, "ngt_data")

COMPANY_FILE = os.path.join(BASE_DIR, "docs", "COMPANIES.csv")
API_CONFIG_FILE = os.path.join(BASE_DIR, "docs", "config.json")



def get_company():
    

# -----------------------
# ENSURE DIRS (optional)
# -----------------------

def ensure_dirs():
    for d in [OUTPUT_DIR, LOG_DIR, DATA_DIR, NGT_DATA_DIR]:
        utils.create_dir(d)

# -----------------------
# CONFIG ACCESS
# -----------------------

MAIL_CONFIG = PATHS.get("mail_data", {})
SCHEDULER_CONFIG = PATHS.get("schedular_data", {})


