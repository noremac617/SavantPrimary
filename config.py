import json
import os

CONFIG_FILE = "./data/settings.json"

def get_allocation():
    if not os.path.exists(CONFIG_FILE):
        return 0.25  # default 25%
    with open(CONFIG_FILE, "r") as f:
        return json.load(f).get("allocation", 0.25)

def set_allocation(value: float):
    config = load_config()
    config["allocation"] = value
    save_config(config)

def get_demo_mode():
    """Get demo mode status"""
    config = load_config()
    return config.get("demo_mode", False)

def set_demo_mode(enabled: bool):
    """Set demo mode status"""
    config = load_config()
    config["demo_mode"] = enabled
    save_config(config)

def load_config():
    """Load full configuration"""
    if not os.path.exists(CONFIG_FILE):
        return {"allocation": 0.25, "demo_mode": False}
    
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"allocation": 0.25, "demo_mode": False}

def save_config(config: dict):
    """Save full configuration"""
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)