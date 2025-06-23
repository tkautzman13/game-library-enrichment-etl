import pandas as pd
from typing import Dict, Any, Optional
from src.utils import get_logger


def extract_library_data(
    config: Dict[str, Any]
) -> None:
    """
    Reads a CSV file containing raw library data from the specified input path,
    and writes it to the specified output path without any modification.

    Parameters:
    -----------
    config
        Configuration dictionary containing data paths and settings.

    Returns:
    --------
    None
    """
    logger = get_logger()

    logger.info('Beginning library source data extraction...')

    # File paths
    input_file = config["data"]["library_source_file"]
    output_file = f'{config["data"]["library_raw_path"]}playnite_library_raw.csv'

    # Read library CSV data from input_file source
    logger.debug('Reading source library data...')
    library_raw_df = pd.read_csv(input_file, skiprows=1, header=0)

    # Write library CSV to output_file
    logger.debug('Writing raw library data...')
    library_raw_df.to_csv(output_file, index=False)

    logger.info(
        f"COMPLETE: Library data successfully pulled from {input_file} and stored in: {output_file}"
    )


def transform_library_data(
    config: Dict[str, Any],
)-> None:
    """
    Reads a CSV file containing library data, cleans and processes the data by:
    - Removing platform suffixes (e.g., '(Xbox)', '(Game Pass)', etc.) from game names.
    - Filtering out games categorized as 'Apps'.
    - Removing entries with missing 'completion_status'.
    - Dropping duplicate games based on 'name' and 'release_date'.
    - Filter out games with 'Ignore' category
    - Add name_no_punct field that removes specific punctuation (-, :)
    - Add Library Release Year field for matching processes
    Finally, saves the cleaned data to the specified output path.

    Parameters:
    -----------
    config
        Configuration dictionary containing data paths and settings.

    Returns:
    --------
    None
    """
    logger = get_logger()

    logger.info('Beginning library data cleaning...')

    # File paths
    input_file = f'{config["data"]["library_raw_path"]}playnite_library_raw.csv'
    output_file = f'{config["data"]["library_processed_path"]}playnite_library.csv'

    logger.debug('Reading raw library data...')
    library_interm_df = pd.read_csv(input_file)

    logger.debug('Filtering and cleaning library data...')
    # Column name changes
    library_interm_df = library_interm_df.rename(
        columns={
            "Name": "name",
            "Id": "id",
            "Hidden": "hidden",
            "Categories": "categories",
            "CompletionStatus": "completion_status", 
            "ReleaseDate": "release_date"
            }
    )

    # Remove (Xbox), (Game Pass), (Switch), (PlayStation) suffix from game names
    suffixes = {" (Xbox)", " (Game Pass)", " (Switch)", " (PlayStation)"}
    replacement = ""

    for suffix in suffixes:
        library_interm_df["name"] = library_interm_df["name"].str.replace(suffix, replacement)

    # Exclude 'Apps' Category games (ie GamePass)
    library_interm_df = library_interm_df[~library_interm_df["categories"].str.contains("Apps", na=False)]

    # Exclude games without a library completion_status
    library_interm_df = library_interm_df[~library_interm_df["completion_status"].isnull()]

    # Drop duplicates
    library_interm_df = library_interm_df.drop_duplicates(subset=["name", "release_date"])

    # Filter out 'HLTB Ignore' flagged records
    library_interm_df = library_interm_df[
        ~library_interm_df["categories"].str.contains("Ignore", na=False)
    ]

    # Fix dashes and colons found in library_data.name
    replacements = {"–": "-", ":": ""}
    library_interm_df["name_no_punct"] = library_interm_df["name"].replace(
        replacements, regex=True
    )

    # Ensure the release_date field is a date
    library_interm_df["release_date"] = pd.to_datetime(library_interm_df["release_date"])

    # Add Library release_dates to the library data
    library_interm_df["library_release_year"] = pd.to_datetime(
        library_interm_df["release_date"]
    ).dt.year

    logger.debug('Writing cleaned library data...')
    
    # Export intermediate data
    library_interm_df.to_csv(output_file, index=False)

    logger.info(f"COMPLETE: Library data successfully cleaned and stored in: {output_file}")
