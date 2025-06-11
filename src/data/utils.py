import yaml
from pathlib import Path
import argparse
import logging
import os
from datetime import datetime

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


def parse_args():
    """
    Parses command-line arguments for the data pipeline script to determine
    which pipeline components should be executed and configuration settings.

    Parameters:
    -----------
    None

    Returns:
    --------
    argparse.Namespace
        Parsed command-line arguments containing boolean flags for each pipeline
        component (library, hltb, igdb, playtime) and the config file path.
    """
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


def setup_logger(name: str = "data_pipeline", log_level: int = logging.INFO, log_dir: str = "logs") -> logging.Logger:
    """
    Set up a logger that writes to both file and console.
    
    Parameters:
        name (str): Logger name
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir (str): Directory to store log files
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Prevent duplicate handlers if logger already exists
    if logger.handlers:
        logger.handlers.clear()
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # File handler - with timestamp in filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{name}_{timestamp}.log"
    log_filepath = os.path.join(log_dir, log_filename)
    
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(detailed_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(simple_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str = 'data_pipeline') -> logging.Logger:
    """
    Get an existing logger or create a new one if it doesn't exist.
    """
    return logging.getLogger(name)