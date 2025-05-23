from howlongtobeatpy import HowLongToBeat, SearchModifiers
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import yaml

# Pull in yaml config file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)


def hltb_prepare_library_data(
    library_interm_file=f'{config["data"]["interm_path"]}library_interm.csv',
    hltb_interm_file=f'{config["data"]["interm_path"]}hltb_interm.csv',
):
    # Import library data
    library_prepped = pd.read_csv(library_interm_file)

    # Fix dashes and colons found in library_data.Name
    replacements = {
        "–": "-",
        ":": ""
    }
    library_prepped["name_no_punct"] = library_prepped["Name"].replace(
        replacements, regex=True
    )

    # Keep only the Name and name_no_punct fields
    library_prepped[["Name", "name_no_punct"]]

    # Ensure the Release Date field is a date
    library_prepped["Release Date"] = pd.to_datetime(library_prepped["Release Date"])

    # Check for hltb_interm.csv - if it exists, pass through only new records or recently released (within past 3 months) games
    hltb_interm_file = Path(hltb_interm_file)
    if hltb_interm_file.is_file():
        hltb_interm = pd.read_csv(hltb_interm_file)
        # Condition 1: Recent Release Date
        cond1 = library_prepped["Release Date"] >= (
            datetime.today() - timedelta(days=90)
        )

        # Condition 2: Game in library is not found in hltb_interm
        cond2 = ~library_prepped["Name"].isin(hltb_interm["Library Name"])

        # Combine both conditions
        library_prepped = library_prepped[cond1 | cond2]
    else:
        pass

    # Return prepped library_data
    return library_prepped


def extract_raw_hltb_data(library_prepped, hltb_raw_path=config["data"]["hltb_raw_path"]):
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

        hltb_raw_df = pd.concat([hltb_raw_df, df])

    # Output hltb_raw dataset
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    hltb_raw_df.to_csv(f"{hltb_raw_path}/hltb_raw_{current_datetime}.csv", index=False)
