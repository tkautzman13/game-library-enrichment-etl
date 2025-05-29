from howlongtobeatpy import HowLongToBeat, SearchModifiers
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import yaml


def prepare_library_data_hltb(
    config_path="config.yaml",
    library_interm_file="library_cleaned.csv",
    # hltb_interm_file="hltb_interm.csv",
    library_hltb_file="library_hltb.csv",
):
    # Load config file
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    print('Reading cleaned library data...')
    # Interm path
    interm_data_path = f'{config["data"]["interm_path"]}'

    print('Preparing library data for HLTB query...')
    # Import library data
    library_prepped = pd.read_csv(interm_data_path + library_interm_file)

    # Filter out 'HLTB Ignore' flagged records
    library_prepped = library_prepped[~library_prepped["Categories"].str.contains("HLTB Ignore", na=False)]

    # Fix dashes and colons found in library_data.Name
    replacements = {"–": "-", ":": ""}
    library_prepped["name_no_punct"] = library_prepped["Name"].replace(
        replacements, regex=True
    )

    # Ensure the Release Date field is a date
    library_prepped["Release Date"] = pd.to_datetime(library_prepped["Release Date"])

    print('Writing prepared library HLTB data...')
    # Export prepped library_data
    library_prepped.to_csv(interm_data_path + library_hltb_file, index=False)

    print(
        f"Library data successfully prepared for HLTB query and stored in: {interm_data_path + library_hltb_file}"
    )


def extract_raw_hltb_data(
    config_path="config.yaml", library_hltb_file="library_hltb.csv"
):
    # Load config file
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Paths
    library_hltb_path = f'{config["data"]["interm_path"]}{library_hltb_file}'
    hltb_raw_path = config["data"]["hltb_raw_path"]

    print('Reading prepared library HLTB data...')
    # Import library_hltb.csv
    library_prepped = pd.read_csv(library_hltb_path)

    # Create empty hltb_df
    hltb_raw_df = pd.DataFrame()

    print('Fetching HLTB data...')
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
    print('HLTB data successfully extracted!')

    print('Writing raw HLTB data...')
    # Output hltb_raw dataset
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    hltb_raw_df.to_csv(f"{hltb_raw_path}/hltb_raw_{current_datetime}.csv", index=False)

    print(
        f"Successfully extracted raw HLTB data and stored in: {hltb_raw_path}/hltb_raw_{current_datetime}.csv"
    )


def load_latest_hltb_raw_data(path):
    """
    Load the most recent HLTB CSV file from the extracts folder.

    Args:
        hltb_raw_path (str): Path to the folder containing HLTB extract CSV files

    Returns:
        pd.DataFrame: The most recent HLTB raw data
    """
    # List all CSV files in the folder
    csv_files = list(Path(path).glob("*.csv"))

    # Filter and find the most recently modified file
    if csv_files:
        most_recent_file = max(csv_files, key=lambda f: f.stat().st_mtime)
        hltb_raw = pd.read_csv(most_recent_file)
        print(f"Loaded most recent file: {most_recent_file.name}")
        return hltb_raw
    else:
        raise FileNotFoundError("No CSV files found in the HLTB extracts folder.")


def load_prepared_library_data(path):
    """
    Load library data and prepare release year column.

    Args:
        interm_path (str): Path to the intermediate data folder

    Returns:
        pd.DataFrame: Library data with prepared release year column
    """
    # Load library data
    library_hltb = pd.read_csv(f"{path}library_hltb.csv")

    # Add Library Release Dates to the library data
    library_hltb["Library Release Year"] = pd.to_datetime(
        library_hltb["Release Date"]
    ).dt.year

    return library_hltb


def select_best_hltb_match(group):
    """
    For each Library ID group, if there's only one record, return it.
    If there are multiple records (same max similarity), pick the one
    with release year closest to the library release year.
    Also detect and flag perfect vs imperfect matches.
    """
    if len(group) == 1:
        return group

    # Multiple records - find best match
    library_year = group["Library Release Year"].iloc[0]
    game_name = group["Name"].iloc[0]  # Game name from library data

    # Calculate absolute difference between HLTB release year and Library release year
    group_copy = group.copy()
    group_copy["year_diff"] = abs(group_copy["release_year"] - library_year)

    # Check if any HLTB record has exact year match
    exact_matches = group_copy[group_copy["year_diff"] == 0]

    if len(exact_matches) > 0:
        # Perfect match found
        best_match = exact_matches.iloc[0:1]  # Take first if multiple exact matches
    else:
        # No exact match - use closest year
        best_match_idx = group_copy["year_diff"].idxmin()
        best_match = group_copy.loc[[best_match_idx]]
        hltb_years = group_copy["release_year"].tolist()

    return best_match.drop(columns=["year_diff"])


def filter_and_match_hltb_data(hltb_raw, library_hltb, verbose=True):
    """
    Filter HLTB data to keep only the best matches and resolve duplicates using release years.

    Args:
        hltb_raw (pd.DataFrame): Raw HLTB query results
        library_hltb (pd.DataFrame): Library data with release years
        verbose (bool): Whether to print matching progress messages

    Returns:
        pd.DataFrame: Filtered HLTB data with best matches selected
    """
    # Keep only hltb records that contain the maximum similarity score for each Library ID
    hltb_filtered = hltb_raw[
        hltb_raw["similarity"]
        == hltb_raw.groupby(by="Library ID", as_index=False)["similarity"].transform(
            "max"
        )
    ]

    # Remove duplicate records
    hltb_filtered = hltb_filtered[~hltb_filtered.duplicated()]

    # Merge with library data to get release years and names
    hltb_with_library = hltb_filtered.merge(
        library_hltb[["Id", "Name", "Library Release Year"]],
        how="right",
        left_on="Library ID",
        right_on="Id",
    )

    if verbose:
        print("Resolving release year mismatches...")
        print("=" * 60)

    # Apply the function to each Library ID group
    hltb_new = hltb_with_library.groupby("Library ID", group_keys=False).apply(
        select_best_hltb_match
    )

    if verbose:
        print("=" * 60)
        print("Matching complete")

    # Reset index to clean up after groupby operations
    hltb_new = hltb_new.reset_index(drop=True)

    return hltb_new


def create_comprehensive_matching_report(hltb_with_library, library_hltb, interm_path):
    """
    Create a comprehensive report of all matching issues for review.

    Args:
        hltb_with_library (pd.DataFrame): HLTB data merged with library data
        library_hltb (pd.DataFrame): Original library data
        interm_path (str): Path to save report CSV files

    Returns:
        dict: Summary statistics of the matching process
    """
    year_mismatches = []
    no_hltb_records = []
    low_similarity_games = []

    # Get all library games for comparison
    all_library_games = library_hltb[["Id", "Name", "Library Release Year"]].copy()

    # 1. Check for games with no HLTB records (missing from merged data)
    merged_library_ids = set(hltb_with_library["Library ID"].dropna())
    all_library_ids = set(all_library_games["Id"])
    missing_library_ids = all_library_ids - merged_library_ids

    for library_id in missing_library_ids:
        game_info = all_library_games[all_library_games["Id"] == library_id].iloc[0]
        no_hltb_records.append(
            {
                "Library ID": library_id,
                "Game Name": game_info["Name"],
                "Library Year": game_info["Library Release Year"],
            }
        )

    # 2. Check for low similarity scores and year mismatches
    for library_id in hltb_with_library["Library ID"].unique():
        if pd.isna(library_id):
            continue

        group = hltb_with_library[hltb_with_library["Library ID"] == library_id]

        if len(group) == 0:
            continue

        game_name = group["Name"].iloc[0]
        library_year = group["Library Release Year"].iloc[0]
        max_similarity = group["similarity"].max()

        # Check for low similarity
        if max_similarity < 0.75:
            low_similarity_games.append(
                {
                    "Library ID": library_id,
                    "Game Name": game_name,
                    "Library Year": library_year,
                    "Max Similarity": max_similarity,
                    "HLTB Match": (
                        group.loc[group["similarity"].idxmax(), "game"]
                        if "game" in group.columns
                        else "Unknown"
                    ),
                }
            )

        # Check for year mismatches (only for games with multiple HLTB records)
        if len(group) > 1:
            hltb_years = group["release_year"].tolist()

            if library_year not in hltb_years:
                year_mismatches.append(
                    {
                        "Library ID": library_id,
                        "Game Name": game_name,
                        "Library Year": library_year,
                        "HLTB Years": hltb_years,
                        "Closest HLTB Year": min(
                            hltb_years, key=lambda x: abs(x - library_year)
                        ),
                    }
                )

    # Print reports
    print("\n" + "=" * 80)
    print("📊 COMPREHENSIVE MATCHING REPORT")
    print("=" * 80)

    # Report 1: Games with no HLTB records
    if no_hltb_records:
        print(
            f"\n❌ {len(no_hltb_records)} games in library with NO HLTB records found:"
        )
        print("-" * 60)
        no_hltb_df = pd.DataFrame(no_hltb_records)
        print(no_hltb_df.to_string(index=False))
        no_hltb_df.to_csv(f"{interm_path}no_hltb_records.csv", index=False)
        print(f"💾 Details saved to: {interm_path}no_hltb_records.csv")
    else:
        print("\n✅ All library games have HLTB records!")

    # Report 2: Games with low similarity scores
    if low_similarity_games:
        print(f"\n⚠️  {len(low_similarity_games)} games with similarity < 0.75:")
        print("-" * 60)
        low_sim_df = pd.DataFrame(low_similarity_games)
        print(low_sim_df.to_string(index=False))
        low_sim_df.to_csv(f"{interm_path}low_similarity_games.csv", index=False)
        print(f"💾 Details saved to: {interm_path}low_similarity_games.csv")
    else:
        print("\n✅ All matched games have similarity ≥ 0.75!")

    # Report 3: Games with year mismatches
    if year_mismatches:
        print(f"\n⚠️  {len(year_mismatches)} games with release year mismatches:")
        print("-" * 60)
        mismatch_df = pd.DataFrame(year_mismatches)
        print(mismatch_df.to_string(index=False))
        mismatch_df.to_csv(f"{interm_path}year_mismatches.csv", index=False)
        print(f"💾 Details saved to: {interm_path}year_mismatches.csv")
    else:
        print("\n✅ No release year mismatches found!")

    # Summary statistics
    total_library_games = len(all_library_games)
    successful_matches = (
        total_library_games - len(no_hltb_records) - len(low_similarity_games)
    )

    print("\n" + "=" * 80)
    print("📈 MATCHING SUMMARY:")
    print(f"   Total library games: {total_library_games}")
    print(
        f"   Successful matches: {successful_matches} ({successful_matches/total_library_games*100:.1f}%)"
    )
    print(f"   No HLTB records: {len(no_hltb_records)}")
    print(f"   Low similarity: {len(low_similarity_games)}")
    print(f"   Year mismatches: {len(year_mismatches)}")
    print("=" * 80)

    return {
        "total_games": total_library_games,
        "successful_matches": successful_matches,
        "no_hltb_records": len(no_hltb_records),
        "low_similarity": len(low_similarity_games),
        "year_mismatches": len(year_mismatches),
    }


def process_hltb_data(config_path="config.yaml", generate_report=True, verbose=True):
    """
    Complete HLTB data processing pipeline.

    Args:
        config_path (str): Path to configuration YAML file
        generate_report (bool): Whether to generate comprehensive matching report
        verbose (bool): Whether to print detailed progress messages

    Returns:
        tuple: (processed_hltb_dataframe, matching_statistics)
    """
    # Load configuration
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    hltb_raw_path = config["data"]["hltb_raw_path"]
    interm_path = config["data"]["interm_path"]

    if verbose:
        print("Starting HLTB data processing pipeline...")

    # Step 1: Load latest HLTB data
    if verbose:
        print("Loading HLTB data...")
    hltb_raw = load_latest_hltb_raw_data(hltb_raw_path)

    # Step 2: Load and prepare library data
    if verbose:
        print("Loading prepared library data...")
    library_hltb = load_prepared_library_data(interm_path)

    # Step 3: Filter and match HLTB data
    if verbose:
        print("Processing HLTB matches...")
    hltb_processed = filter_and_match_hltb_data(hltb_raw, library_hltb, verbose=verbose)

    # Step 4: Generate comprehensive report (optional)
    matching_stats = None
    if generate_report:
        # Need to recreate hltb_with_library for reporting
        hltb_filtered = hltb_raw[
            hltb_raw["similarity"]
            == hltb_raw.groupby(by="Library ID", as_index=False)[
                "similarity"
            ].transform("max")
        ]
        hltb_filtered = hltb_filtered[~hltb_filtered.duplicated()]
        hltb_with_library = hltb_filtered.merge(
            library_hltb[["Id", "Name", "Library Release Year"]],
            how="right",
            left_on="Library ID",
            right_on="Id",
        )

        matching_stats = create_comprehensive_matching_report(
            hltb_with_library, library_hltb, interm_path
        )

    if verbose:
        print(f"Pipeline complete! Processed {len(hltb_processed)} HLTB records.")

    hltb_processed.to_csv(f"{interm_path}hltb_cleaned.csv")

    if verbose:
        print(
            f"HLTB data successfully processed and stored in: {interm_path}hltb_cleaned.csv"
        )
