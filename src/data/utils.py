import yaml
from pathlib import Path
import argparse
import logging

def load_config(config_path: str) -> dict:
    """
    Loads and validates a YAML configuration file from the specified path.
    Raises appropriate errors if the file is missing, empty, or invalid.

    Parameters:
    -----------
    config_path
        Path to the configuration YAML file.

    Returns:
    --------
    dict
        Dictionary containing the loaded configuration data.
    """
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



def parse_data_pipeline_args():
    parser = argparse.ArgumentParser(description='Run the data pipeline with configurable components')
    
    # Add arguments for each pipeline component
    parser.add_argument('--library', action='store_true', default=False,
                       help='Run the library data pipeline (default: False)')
    parser.add_argument('--hltb', action='store_true', default=False,
                       help='Run the HowLongToBeat data pipeline (default: False)')
    parser.add_argument('--igdb', action='store_true', default=False,
                       help='Run the IGDB data pipeline (default: False)')
    parser.add_argument('--playtime', action='store_true', default=False,
                       help='Run the playtime history pipeline (default: False)')
    
    # Special flag to run all components (maintains backward compatibility)
    parser.add_argument('--all', action='store_true', default=False,
                       help='Run all pipeline components')
    
    # Config file argument
    parser.add_argument('--config', type=str, default='config.yaml',
                       help='Path to config file (default: config.yaml)')
    
    args = parser.parse_args()
    
    # If --all is specified, set all components to True
    if args.all:
        args.library = True
        args.hltb = True
        args.igdb = True
        args.playtime = True
    
    # If no specific components are selected and --all is not used, run all by default
    if not any([args.library, args.hltb, args.igdb, args.playtime]):
        args.library = True
        args.hltb = True
        args.igdb = True
        args.playtime = True
    
    return args