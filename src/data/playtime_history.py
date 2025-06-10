import pandas as pd
from typing import Dict, Any, Optional

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

    print('Beginning playtime source data extraction...')

    # Files
    input_file = config["data"]["playtime_source_file"]
    output_file = f'{config["data"]["raw_path"]}playtime_raw.csv'

    playtime_raw_df = pd.read_csv(input_file, encoding='latin1')

    print('Writing raw playtime data...')
    playtime_raw_df.to_csv(output_file, index=False)

    print(
        f"Complete: playtime data successfully pulled from {input_file} and stored in: {output_file}."
    )