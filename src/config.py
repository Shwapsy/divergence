import os
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
SETTINGS_FILE = DATA_DIR / "settings.json"
MUTED_COINS_FILE = DATA_DIR / "muted_coins.json"

DEFAULT_SETTINGS = {
    "threshold_percent": 1.0,
    "check_interval_seconds": 60,
    "binance_enabled": True,
    "bybit_enabled": True,
    "gate_enabled": True,
    "chat_ids": []
}

def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        with open(SETTINGS_FILE, "r") as f:
            saved = json.load(f)
            settings = DEFAULT_SETTINGS.copy()
            settings.update(saved)
            return settings
    return DEFAULT_SETTINGS.copy()

def save_settings(settings: dict):
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)

def load_muted_coins() -> dict:
    if MUTED_COINS_FILE.exists():
        with open(MUTED_COINS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_muted_coins(muted: dict):
    with open(MUTED_COINS_FILE, "w") as f:
        json.dump(muted, f, indent=2)
