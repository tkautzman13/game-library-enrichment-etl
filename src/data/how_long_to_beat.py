from howlongtobeatpy import HowLongToBeat, SearchModifiers
import pandas as pd
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from typing import Dict, Any, Optional
from data.utils import get_logger


def extract_hltb_data(
    config: Dict[str, Any]
) -> None:
    """
    Queries HowLongToBeat and extracts raw time-to-beat data for each game in the library.
    Saves the raw data to a timestamped CSV file.

    Parameters:
    -----------
    config : Dict[str, Any]
        Configuration dictionary containing data paths and settings.

    Returns:
    --------
    None
    """
    logger = get_logger()

    logger.info("Beginning HLTB data extraction...")

    # File Paths
    library_cleaned_file = f'{config["data"]["interm_path"]}library_cleaned.csv'
    hltb_raw_path = config["data"]["hltb_raw_path"]

    logger.debug("Reading prepared library data...")

    # Import _library_df_prep.csv
    library_df = pd.read_csv(library_cleaned_file)

    all_hltb_data = []

    logger.info("Fetching HLTB data...")
    # For loop to query HLTB data
    for index, row in tqdm(library_df.iterrows(), total=len(library_df)):
        # Prepare the search name
        search_name = row["name_no_punct"]
        
        # If the name starts with 'Pokémon', remove 'Version' from it
        if search_name.startswith('Pokémon'):
            search_name = search_name.replace('Version', '').strip()
        
        # Get results_list (Check if game is marked as "DLC")
        if isinstance(row["Categories"], str) and "DLC" in row["Categories"]:
            results_list = HowLongToBeat().search(
                search_name, similarity_case_sensitive=False
            )
        else:
            results_list = HowLongToBeat().search(
                search_name,
                similarity_case_sensitive=False,
                search_modifiers=SearchModifiers.HIDE_DLC,
            )

        # Process all results for this game
        for element in results_list:
            row_data = {
                "game": element.game_name,
                "release_year": element.release_world,
                "similarity": element.similarity,
                "hltb_main": element.main_story,
                "hltb_extra": (element.main_extra - element.main_story),
                "hltb_completionist": (element.completionist - element.main_story),
                "Library Name": row["Name"],
                "Library ID": row["Id"]
            }
            all_hltb_data.append(row_data)

    # Create DataFrame once from all collected data
    hltb_raw_df = pd.DataFrame(all_hltb_data)

    logger.info("HLTB data successfully extracted")

    logger.debug("Writing raw HLTB data...")
    # Output hltb_raw dataset
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    hltb_raw_df.to_csv(f"{hltb_raw_path}/hltb_raw_{current_datetime}.csv", index=False)

    logger.info(
        f"COMPLETE: Successfully extracted raw HLTB data and stored in: {hltb_raw_path}/hltb_raw_{current_datetime}.csv"
    )


def transform_hltb_data(
    config: Dict[str, Any], 
    generate_report: bool = True
) -> None:
    """
    Executes the full processing pipeline for HowLongToBeat data integration.
    Loads raw HLTB data, processes matches with library data, and saves cleaned results.

    Parameters:
    -----------
    config : Dict[str, Any]
        Configuration dictionary containing data paths and settings.
    generate_report : bool
        Whether to generate a comprehensive matching report.

    Returns:
    --------
    None
    """
    logger = get_logger()
    
    logger.info("Beginning HLTB data processing...")

    hltb_raw_path = config["data"]["hltb_raw_path"]
    interm_path = config["data"]["interm_path"]
    hltb_issues_report_path = config["data"]["hltb_issues_report_path"]
    library_cleaned_file = f'{interm_path}library_cleaned.csv'

    # Step 1: Load latest HLTB data
    logger.debug("Loading HLTB data...")
    hltb_raw_df = load_latest_hltb_raw_data(hltb_raw_path)

    # Step 2: Load and prepare library data
    logger.debug("Loading prepared library data...")
    library_df = pd.read_csv(library_cleaned_file)

    # Step 3: Filter and match HLTB data
    logger.debug("Processing HLTB matches...")
    hltb_processed_df = filter_and_match_hltb_data(hltb_raw_df, library_df)

    # Step 4: Generate comprehensive report (optional)
    if generate_report:
        # Need to recreate hltb_with_library for reporting
        hltb_filtered_df = hltb_raw_df[
            hltb_raw_df["similarity"]
            == hltb_raw_df.groupby(by="Library ID", as_index=False)[
                "similarity"
            ].transform("max")
        ]
        hltb_filtered_df = hltb_filtered_df[~hltb_filtered_df.duplicated()]
        hltb_with_library_df = hltb_filtered_df.merge(
            library_df[["Id", "Name", "Library Release Year"]],
            how="right",
            left_on="Library ID",
            right_on="Id",
        )

        create_comprehensive_matching_report(
            hltb_with_library_df, library_df, hltb_issues_report_path
        )

    hltb_processed_df[
        [
            "Library Name",
            "Library ID",
            "Library Release Year",
            "hltb_main",
            "hltb_extra",
            "hltb_completionist",
        ]
    ].to_csv(
        f"{interm_path}hltb_cleaned.csv",
        index=False,
    )

    logger.info(
        f"COMPLETE: HLTB data successfully processed and stored in: {interm_path}hltb_cleaned.csv"
    )


def load_latest_hltb_raw_data(
    path: str
) -> pd.DataFrame:
    """
    Loads the most recent HLTB raw data CSV file from a specified folder
    based on file modification time.

    Parameters:
    -----------
    path : str
        Path to the folder containing HLTB extract CSV files.

    Returns:
    --------
    pd.DataFrame
        The most recently created HLTB raw data.
    """
    logger = get_logger()

    # List all CSV files in the folder
    csv_files = list(Path(path).glob("*.csv"))

    # Filter and find the most recently modified file
    if csv_files:
        most_recent_file = max(csv_files, key=lambda f: f.stat().st_mtime)
        hltb_raw = pd.read_csv(most_recent_file)
        logger.debug(f"Loaded most recent file: {most_recent_file.name}")
        return hltb_raw
    else:
        raise FileNotFoundError("No CSV files found in the HLTB extracts folder.")


def select_best_hltb_match(
    group: pd.DataFrame
) -> pd.DataFrame:
    """
    Selects the best match from HLTB results based on release year similarity
    when multiple matches exist for a single library game.

    Parameters:
    -----------
    group : pd.DataFrame
        A grouped subset of HLTB data corresponding to a single Library ID.

    Returns:
    --------
    pd.DataFrame
        The best-matching record(s) for the given Library ID group.
    """
    logger = get_logger()

    if len(group) == 1:
        return group

    # Multiple records - find best match
    library_year = group["Library Release Year"].iloc[0]

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

    return best_match.drop(columns=["year_diff"])


def filter_and_match_hltb_data(
    hltb_raw_df: pd.DataFrame, 
    library_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Filters and matches raw HLTB data with the library, resolving duplicates 
    using release years and similarity scores.

    Parameters:
    -----------
    hltb_raw_df : pd.DataFrame
        Raw HLTB query results.
    library_df : pd.DataFrame
        Cleaned library data.

    Returns:
    --------
    pd.DataFrame
        Filtered and matched HLTB data.
    """
    logger = get_logger()

    # Keep only hltb records that contain the maximum similarity score for each Library ID
    hltb_filtered_df = hltb_raw_df[
        hltb_raw_df["similarity"]
        == hltb_raw_df.groupby(by="Library ID", as_index=False)["similarity"].transform(
            "max"
        )
    ]

    # Remove duplicate records
    hltb_filtered_df = hltb_filtered_df[~hltb_filtered_df.duplicated()]

    # Merge with library data to get release years and names
    hltb_with_library_df = hltb_filtered_df.merge(
        library_df[["Id", "Name", "Library Release Year"]],
        how="right",
        left_on="Library ID",
        right_on="Id",
    )

    logger.debug("Resolving release year mismatches...")

    # Apply the function to each Library ID group
    hltb_new_df = hltb_with_library_df.groupby("Library ID", group_keys=False).apply(
        select_best_hltb_match
    )

    logger.debug("COMPLETE: Matching complete.")

    # Reset index to clean up after groupby operations
    hltb_new_df = hltb_new_df.reset_index(drop=True)

    return hltb_new_df


def create_comprehensive_matching_report(
    hltb_with_library_df: pd.DataFrame, 
    library_df: pd.DataFrame, 
    output_path: str
) -> None:
    """
    Creates a comprehensive report on the quality of matches between HLTB and library data.
    Generates CSV files for games with issues and prints summary statistics.

    Parameters:
    -----------
    hltb_with_library_df : pd.DataFrame
        HLTB data merged with library data for analysis.
    library_df : pd.DataFrame
        Original library data used as reference.
    hltb_issues_report_path : str
        Path to save output report files.

    Returns:
    --------
    None
    """
    logger = get_logger()

    year_mismatches = []
    no_hltb_records = []
    low_similarity_games = []

    # Get all library games for comparison
    all_library_games = library_df[["Id", "Name", "Library Release Year"]].copy()

    # 1. Check for games with no HLTB records (missing from merged data)
    merged_library_ids = set(hltb_with_library_df["Library ID"].dropna())
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
    for library_id in hltb_with_library_df["Library ID"].unique():
        if pd.isna(library_id):
            continue

        group = hltb_with_library_df[hltb_with_library_df["Library ID"] == library_id]

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
    logger.info("=" * 80)
    logger.info("COMPREHENSIVE MATCHING REPORT")
    logger.info("=" * 80)

    # Report 1: Games with no HLTB records
    if no_hltb_records:
        logger.info(
            f"{len(no_hltb_records)} games in library with NO HLTB records found:"
        )
        logger.info("-" * 60)
        no_hltb_df = pd.DataFrame(no_hltb_records)
        no_hltb_df_str = no_hltb_df.to_string(index=False)
        for line in no_hltb_df_str.split('\n'):
            logger.info(line)
        logger.info("-" * 60)
        no_hltb_df.to_csv(f"{output_path}no_hltb_records.csv", index=False)
        logger.info(f"Details saved to: {output_path}no_hltb_records.csv")
    else:
        logger.info("All library games have HLTB records!")

    # Report 2: Games with low similarity scores
    if low_similarity_games:
        logger.info(f"{len(low_similarity_games)} games with similarity < 0.75:")
        logger.info("-" * 60)
        low_sim_df = pd.DataFrame(low_similarity_games)
        low_sim_df_str = low_sim_df.to_string(index=False)
        for line in low_sim_df_str.split('\n'):
            logger.info(line)
        logger.info("-" * 60)
        low_sim_df.to_csv(f"{output_path}low_similarity_games.csv", index=False)
        logger.info(f"Details saved to: {output_path}low_similarity_games.csv")
    else:
        logger.info("All matched games have similarity >= 0.75!")

    # Report 3: Games with year mismatches
    if year_mismatches:
        logger.info(f"{len(year_mismatches)} games with release year mismatches:")
        logger.info("-" * 60)
        mismatch_df = pd.DataFrame(year_mismatches)
        mismatch_df_str = mismatch_df.to_string(index=False)
        for line in mismatch_df_str.split('\n'):
            logger.info(line)
        logger.info("-" * 60)
        mismatch_df.to_csv(f"{output_path}year_mismatches.csv", index=False)
        logger.info(f"Details saved to: {output_path}year_mismatches.csv")
    else:
        logger.info("No release year mismatches found!")

    # Summary statistics
    total_library_games = len(all_library_games)
    successful_matches = (
        total_library_games - len(no_hltb_records) - len(low_similarity_games)
    )

    logger.info("=" * 80)
    logger.info("MATCHING SUMMARY:")
    logger.info(f"   Total library games: {total_library_games}")
    logger.info(
        f"   Successful matches: {successful_matches} ({successful_matches/total_library_games*100:.1f}%)"
    )
    logger.info(f"   No HLTB records: {len(no_hltb_records)}")
    logger.info(f"   Low similarity: {len(low_similarity_games)}")
    logger.info(f"   Year mismatches: {len(year_mismatches)}")
    logger.info("=" * 80)