import pandas as pd
import yaml

# Pull in yaml config file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)


def copy_raw_library_data(
    input_file=config["data"]["library_source_file"],
    output_file=f'{config["data"]["raw_path"]}library_raw.csv',
):
    """
    Reads a CSV file containing raw library data from the specified input path,
    and writes it to the specified output path without any modification.

    Parameters:
    -----------
    input_file : str
        The file path to the input CSV file containing the raw library data.
    output_file : str
        The file path where the raw data CSV will be saved.

    Returns:
    --------
    None
    """
    library_raw = pd.read_csv(input_file)
    library_raw.to_csv(output_file, index=False)

    print(
        f"Library data successfully pulled from {input_file} and stored in: {output_file}"
    )


def process_library_data(
    input_file=f'{config["data"]["raw_path"]}library_raw.csv',
    output_file=f'{config["data"]["interm_path"]}library_interm.csv',
):
    """
    Reads a CSV file containing library data, cleans and processes the data by:
    - Removing platform tags (e.g., '(Xbox)', '(Game Pass)', etc.) from game names.
    - Filtering out games categorized as 'Apps'.
    - Removing entries with missing 'Completion Status'.
    - Dropping duplicate games based on 'Name' and 'Release Date'.
    Finally, saves the cleaned data to the specified output path.

    Parameters:
    -----------
    input_file : str
        The file path to the input CSV file containing raw or intermediate library data.
    output_file : str
        The file path where the cleaned and processed CSV data will be saved.

    Returns:
    --------
    None
    """
    library_interm = pd.read_csv(input_file)

    # Remove (Xbox), (Game Pass), (Switch), (PlayStation) tags from game names
    tags = {" (Xbox)", " (Game Pass)", " (Switch)", " (PlayStation)"}
    replacement = ""

    for tag in tags:
        library_interm["Name"] = library_interm["Name"].str.replace(tag, replacement)

    # Exclude 'Apps' Category games (ie GamePass)
    library_interm[library_interm["Categories"] != "Apps"]

    # Exclude games without a library Completion Status
    library_interm = library_interm[~library_interm["Completion Status"].isnull()]

    # Exclude hidden games
    library_interm = library_interm[library_interm["Hidden"] == False]

    # Drop duplicates
    library_interm = library_interm.drop_duplicates(subset=["Name", "Release Date"])

    # Export intermediate data
    library_interm.to_csv(output_file, index=False)

    print(f"Library data successfully processed and stored in: {output_file}")
