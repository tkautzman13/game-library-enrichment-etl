from howlongtobeatpy import HowLongToBeat, SearchModifiers
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import yaml

def prepare_library_data_hltb(
    config_path='config.yaml',
    library_interm_file="library_cleaned.csv",
    # hltb_interm_file="hltb_interm.csv",
    library_hltb_file="library_hltb.csv"
):
    # Load config file
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Interm path
    interm_data_path=f'{config["data"]["interm_path"]}',

    # Import library data
    library_prepped = pd.read_csv(interm_data_path + library_interm_file)

    # Fix dashes and colons found in library_data.Name
    replacements = {"–": "-", ":": ""}
    library_prepped["name_no_punct"] = library_prepped["Name"].replace(
        replacements, regex=True
    )

    # Ensure the Release Date field is a date
    library_prepped["Release Date"] = pd.to_datetime(library_prepped["Release Date"])

    # # Check for hltb_interm.csv - if it exists, pass through only new records or recently released (within past 3 months) games
    # hltb_interm_file = Path(interm_data_path + hltb_interm_file)
    # if hltb_interm_file.is_file():
    #     hltb_interm_df = pd.read_csv(hltb_interm_file)
    #     # Condition 1: Recent Release Date
    #     cond1 = library_prepped["Release Date"] >= (
    #         datetime.today() - timedelta(days=90)
    #     )

    #     # Condition 2: Game in library is not found in hltb_interm
    #     cond2 = ~library_prepped["Name"].isin(hltb_interm_df["Library Name"])

    #     # Combine both conditions
    #     library_prepped = library_prepped[cond1 | cond2]
    # else:
    #     pass

    # Export prepped library_data
    library_prepped.to_csv(interm_data_path + library_hltb_file, index=False)

    print(f"Library data successfully prepared for HLTB query and stored in: {interm_data_path + library_hltb_file}")


def extract_raw_hltb_data(
    config_path='config.yaml',
    library_hltb_file='library_hltb.csv'
):
    # Load config file
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Paths
    library_hltb_path=f'{config["data"]["interm_path"]}{library_hltb_file}' 
    hltb_raw_path=config["data"]["hltb_raw_path"]

    # Import library_hltb.csv
    library_prepped = pd.read_csv(library_hltb_path)

    # Create empty hltb_df
    hltb_raw_df = pd.DataFrame()

    # For loop to query HLTB data
    for index, row in library_prepped.iterrows():
        # Get results_list (Check if game is marked as "DLC")
        if isinstance(row["Categories"], str) and "DLC" in row["Categories"]:
            results_list = HowLongToBeat().search(
                row["name_no_punct"], similarity_case_sensitive=False
            )
        else:
            results_list = HowLongToBeat().search(
                row["name_no_punct"],
                similarity_case_sensitive=False,
                search_modifiers=SearchModifiers.HIDE_DLC,
            )

        # Convert results_list to dataframe
        data = []
        for element in results_list:
            row_data = {
                "game": element.game_name,
                "release_year": element.release_world,
                "similarity": element.similarity,
                "hltb_main": element.main_story,
                "hltb_extra": (element.main_extra - element.main_story),
                "hltb_completionist": (element.completionist - element.main_story),
            }

            data.append(row_data)

            df = pd.DataFrame(data)

            # Add Playnite game name to dataframe
            df["Library Name"] = row["Name"]
            df["Library ID"] = row["Id"]

        hltb_raw_df = pd.concat([hltb_raw_df, df])

    # Output hltb_raw dataset
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    hltb_raw_df.to_csv(f"{hltb_raw_path}/hltb_raw_{current_datetime}.csv", index=False)

    print(f"Successfully extracted raw HLTB data and stored in: {hltb_raw_path}/hltb_raw_{current_datetime}.csv")
