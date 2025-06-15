import pandas as pd
from typing import Dict, Any, Optional
from utils import get_logger

def extract_playtime_data(
    config: Dict[str, Any]
) -> None:
    """
    Reads a CSV file containing raw playtime data from the specified input path,
    and writes it to the specified output path without any modification.

    Parameters:
    -----------
    config : Dict[str, Any]
        Configuration dictionary containing data paths and settings.

    Returns:
    --------
    None
    """
    logger = get_logger()

    logger.info('Beginning playtime source data extraction...')

    # File paths
    input_file = config["data"]["playtime_source_file"]
    output_file = f'{config["data"]["raw_path"]}playtime_raw.csv'

    # Read playtime data from input_file path
    playtime_raw_df = pd.read_csv(input_file, encoding='latin1')

    # Load playtime data to output_file
    logger.debug('Writing raw playtime data...')
    playtime_raw_df.to_csv(output_file, index=False)

    logger.info(
        f"COMPLETE: playtime data successfully pulled from {input_file} and stored in: {output_file}"
    )