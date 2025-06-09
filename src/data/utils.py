import yaml
from pathlib import Path

def load_config(config_path: str) -> dict:
    """Load and validate configuration file."""
    try:
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        
        if not config:
            raise ValueError("Config file is empty or invalid")
        
        return config
    except Exception as e:
        print(f"Failed to load config: {e}")
        raise