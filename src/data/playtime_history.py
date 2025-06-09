import pandas as pd

def extract_playtime_data(
    config
):
    """
    Reads a CSV file containing raw playtime data from the specified input path,
    and writes it to the specified output path without any modification.

    Parameters:
    -----------
    config_path : str
        Path to the configuration YAML file.

    Returns:
    --------
    None
    """

    print('Beginning playtime source data extraction...')

    # Files
    input_file = config["data"]["playtime_source_file"]
    output_file = f'{config["data"]["raw_path"]}playtime_raw.csv'

    playtime_raw = pd.read_csv(input_file, encoding='latin1')

    print('Writing raw playtime data...')
    playtime_raw.to_csv(output_file, index=False)

    print(
        f"Complete: playtime data successfully pulled from {input_file} and stored in: {output_file}."
    )