import pandas as pd


def extract_library_data(
    config
):
    """
    Reads a CSV file containing raw library data from the specified input path,
    and writes it to the specified output path without any modification.

    Parameters:
    -----------
    config_path : str
        Path to the configuration YAML file.

    Returns:
    --------
    None
    """

    print('Beginning library source data extraction...')

    # Files
    input_file = config["data"]["library_source_file"]
    output_file = f'{config["data"]["raw_path"]}library_raw.csv'

    print('Reading source library data...')
    library_raw = pd.read_csv(input_file, skiprows=1, header=0)

    print('Writing raw library data...')
    library_raw.to_csv(output_file, index=False)

    print(
        f"Complete: Library data successfully pulled from {input_file} and stored in: {output_file}."
    )


def transform_library_data(
    config,
):
    """
    Reads a CSV file containing library data, cleans and processes the data by:
    - Removing platform tags (e.g., '(Xbox)', '(Game Pass)', etc.) from game names.
    - Filtering out games categorized as 'Apps'.
    - Removing entries with missing 'Completion Status'.
    - Dropping duplicate games based on 'Name' and 'Release Date'.
    - Filter out games with 'Ignore' flag
    - Add name_no_punct field that removes specific punctuation (-, :)
    - Add Library Release Year field for matching processes
    Finally, saves the cleaned data to the specified output path.

    Parameters:
    -----------
    config_path : str
        Path to the configuration YAML file.

    Returns:
    --------
    None
    """
    print('Beginning library data cleaning...')

    # Files
    input_file = f'{config["data"]["raw_path"]}library_raw.csv'
    output_file = f'{config["data"]["interm_path"]}library_cleaned.csv'

    print('Reading raw library data...')
    library_interm = pd.read_csv(input_file)

    print('Filtering and cleaning library data...')
    # Column name changes
    library_interm = library_interm.rename(
        columns={"CompletionStatus": "Completion Status", "ReleaseDate": "Release Date"}
    )

    # Remove (Xbox), (Game Pass), (Switch), (PlayStation) tags from game names
    tags = {" (Xbox)", " (Game Pass)", " (Switch)", " (PlayStation)"}
    replacement = ""

    for tag in tags:
        library_interm["Name"] = library_interm["Name"].str.replace(tag, replacement)

    # Exclude 'Apps' Category games (ie GamePass)
    library_interm = library_interm[~library_interm["Categories"].str.contains("Apps", na=False)]

    # Exclude games without a library Completion Status
    library_interm = library_interm[~library_interm["Completion Status"].isnull()]

    # Drop duplicates
    library_interm = library_interm.drop_duplicates(subset=["Name", "Release Date"])

    # Filter out 'HLTB Ignore' flagged records
    library_interm = library_interm[
        ~library_interm["Categories"].str.contains("Ignore", na=False)
    ]

    # Fix dashes and colons found in library_data.Name
    replacements = {"–": "-", ":": ""}
    library_interm["name_no_punct"] = library_interm["Name"].replace(
        replacements, regex=True
    )

    # Ensure the Release Date field is a date
    library_interm["Release Date"] = pd.to_datetime(library_interm["Release Date"])

    # Add Library Release Dates to the library data
    library_interm["Library Release Year"] = pd.to_datetime(
        library_interm["Release Date"]
    ).dt.year

    print('Writing cleaned library data...')
    # Export intermediate data
    library_interm.to_csv(output_file, index=False)

    print(f"Complete: Library data successfully cleaned and stored in: {output_file}.")
