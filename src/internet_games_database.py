import os
import requests
import pandas as pd
import json
from igdb.wrapper import IGDBWrapper
import time
import pandas as pd
from fuzzywuzzy import fuzz, process
from tqdm import tqdm
from typing import Dict, Any, List, Tuple, Optional, Union
from utils import get_logger


def connect_to_igdb(config: Dict[str, Any]) -> IGDBWrapper:
    """
    Establishes a connection to the IGDB (Internet Game Database) API using OAuth2 authentication.
    
    Retrieves an access token from Twitch OAuth service and creates an IGDBWrapper instance
    for making API requests to the IGDB database.
    
    Parameters:
    -----------
    config : str
        Configuration dictionary containing IGDB API credentials with keys:
        - 'igdb_api': dict with 'client_id' and 'client_secret'
    
    Returns:
    --------
    IGDBWrapper
        Authenticated IGDB wrapper instance for making API requests.
    """
    logger = get_logger()

    logger.info('Beginning IGDB connection setup...')

    try:
        client_id = config['igdb_api']['client_id']
        client_secret = config['igdb_api']['client_secret']
        grant_type = 'client_credentials'

        url = f'https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type={grant_type}'
        response = requests.post(url, timeout=30)
        
        # Use raise_for_status() to convert HTTP errors to exceptions
        response.raise_for_status()
        
        token = response.json().get('access_token')
        if not token:
            logger.error("Access token not found in response")
            raise ValueError("Access token not found in response")
        
        logger.info('Connection successful, access token received')
        return IGDBWrapper(client_id, token)
            
    except Exception as e:
        logger.error(f"Error connecting to IGDB: {e}")
        raise


def test_igdb_connection(connection: IGDBWrapper) -> bool:
    """
    Tests the IGDB API connection by making a simple request to verify connectivity.
    
    Performs a test query to the games endpoint to ensure the connection and
    authentication are working properly.
    
    Parameters:
    -----------
    connection : IGDBWrapper
        Authenticated IGDB wrapper instance to test.
    
    Returns:
    --------
    bool
        True if connection test succeeds, False otherwise.
    """
    logger = get_logger()

    logger.info('Beginning IGDB connection test...')
    try:
        response = connection.api_request(
            'games',
            'fields name; limit 1;'
        )
        # Optional: validate the response has expected structure
        if response:
            logger.info('Connection test succeeded!')
            return True
        else:
            logger.warning('Connection test returned empty response')
            return False
    except Exception as e:
        logger.error(f"IGDB connection test failed: {e}")
        return False


def execute_igdb_query(connection: IGDBWrapper, endpoint: str, query_where: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Executes a paginated query against a specified IGDB API endpoint.
    
    Retrieves all available data from an IGDB endpoint by making multiple requests
    with pagination (500 records per request) until all data is collected.
    
    Parameters:
    -----------
    connection : IGDBWrapper
        Authenticated IGDB wrapper instance for making API requests.
    endpoint : str
        IGDB API endpoint name (e.g., 'games', 'genres', 'themes').
    query_where : Optional[str], default=None
        Optional WHERE clause to filter the query results.
    
    Returns:
    --------
    List[Dict[str, Any]]
        List of dictionaries containing all records from the specified endpoint.
    """
    logger = get_logger()

    o=0
    data=[]
    
    try:
        # Fetch data
        while True:
            if query_where:
                json_results = connection.api_request(
                    f'{endpoint}',
                    f'fields *; limit 500; offset {o};{query_where}'
                )
            else:
                json_results = connection.api_request(
                    f'{endpoint}',
                    f'fields *; limit 500; offset {o};'
                )            

            json_load = json.loads(json_results)

            data.extend(json_load)

            o += 500

            if len(json_load) < 500:
                break

        return data
    
    except Exception as e:
        logger.error('Error fetching IGDB data: {e}')
        raise


def extract_and_update_igdb_data(connection: IGDBWrapper, config: Dict[str, Any]) -> None:
    """
    Orchestrates the extraction and updating of IGDB data for multiple endpoints.
    
    Manages the complete data extraction process for various IGDB endpoints,
    performing either full data loads for new endpoints or incremental updates
    for existing data files.
    
    Parameters:
    -----------
    connection : IGDBWrapper
        Authenticated IGDB wrapper instance for making API requests.
    config : Dict[str, Any]
        Configuration dictionary containing data paths and settings with key:
        - 'data': dict with 'igdb_raw_path' for output directory
    
    Returns:
    --------
    None
    """
    logger = get_logger()

    logger.info('Beginning IGDB extracts/updates...')

    # Specify parameters
    igdb_raw_path = config['data']['igdb_raw_path']
    endpoint_list = [
        'games', 'franchises', 'game_types', 'genres', 'themes', 'keywords', 'player_perspectives'
    ]

    # Test IGDB Connection before running
    if test_igdb_connection(connection):
        for endpoint in endpoint_list:
            logger.info("=" * 60)
            logger.info(f" {endpoint}")
            logger.info("=" * 60)
            if os.path.isfile(f'{igdb_raw_path}igdb_{endpoint}.csv'):
                logger.info(f'{endpoint}.csv already exists. Collecting updates and new records...')
                extract_igdb_data_new(connection=connection, endpoint=endpoint, config=config)
                time.sleep(0.5)
                update_igdb_data(connection=connection, endpoint=endpoint, config=config)
                time.sleep(0.5)
            else:
                logger.info(f'{endpoint}.csv does not exist. Performing full data load...')
                extract_igdb_data_full(connection=connection, endpoint=endpoint, config=config)
                # Pause for 1 second to avoid too many requests
                time.sleep(0.5)

    logger.info('COMPLETE: IGDB data successfully updated/extracted')


def extract_igdb_data_full(connection: IGDBWrapper, endpoint: str, config: Dict[str, Any]) -> None:
    """
    Performs a complete data extraction from a specified IGDB endpoint.
    
    Retrieves all available data from an IGDB endpoint and saves it as a CSV file.
    Used when no existing data file is present for the endpoint.
    
    Parameters:
    -----------
    connection : IGDBWrapper
        Authenticated IGDB wrapper instance for making API requests.
    endpoint : str
        IGDB API endpoint name to extract data from.
    config : Dict[str, Any]
        Configuration dictionary containing output path with key:
        - 'data': dict with 'igdb_raw_path' for CSV output directory
    
    Returns:
    --------
    None
    """
    logger = get_logger()

    logger.info(f"Beginning IGDB data extraction from the {endpoint} endpoint...")

    # Specify parameters
    output_path=config['data']['igdb_raw_path']

    logger.debug(f'Fetching data from {endpoint} endpoint...')
    # Fetch games data
    data = execute_igdb_query(connection=connection, endpoint=endpoint)

    logger.info(f'{len(data)} rows successfully retrieved from {endpoint}')

    df = pd.DataFrame(data)

    df.to_csv(f'{output_path}igdb_{endpoint}.csv', index=False)
    logger.info(f'COMPLETE: {endpoint} endpoint data successfully written to {output_path}igdb_{endpoint}.csv')


def update_igdb_data(connection: IGDBWrapper, config: Dict[str, Any], endpoint: str) -> None:
    """
    Updates existing IGDB data with recently modified records from the API.
    
    Reads the existing CSV file for an endpoint, identifies the most recent update
    timestamp, and retrieves only records that have been updated since then.
    
    Parameters:
    -----------
    connection : IGDBWrapper
        Authenticated IGDB wrapper instance for making API requests.
    config : Dict[str, Any]
        Configuration dictionary containing data paths with key:
        - 'data': dict with 'igdb_raw_path' for CSV file directory
    endpoint : str
        IGDB API endpoint name to update data for.
    
    Returns:
    --------
    None
    """
    logger = get_logger()

    logger.info(f"Beginning IGDB updated data retrieval from the {endpoint} endpoint...")

    # Specify parameters
    igdb_raw_path = config['data']['igdb_raw_path']
    csv_path=f'{igdb_raw_path}igdb_{endpoint}.csv'

    # Read endpoint CSV and collect max update and created timestamps
    logger.debug(f'Loading {endpoint}.csv...')
    endpoint_df = pd.read_csv(csv_path, low_memory=False).set_index('id')
    max_update_ts = endpoint_df['updated_at'].max()
    query_where = f'where updated_at > {max_update_ts};'

    logger.debug(f'Fetching updates from {endpoint} endpoint...')
    # Fetch updated data
    update_data = execute_igdb_query(connection=connection, endpoint=endpoint, query_where=query_where)

    # Check for new rows
    if len(update_data) == 0:
        logger.info('No updated rows retrieved. File not updated.')
    else:
        logger.info(f'{len(update_data)} updated rows successfully retrieved from {endpoint} endpoint')

        # Setup new dataframes
        update_df = pd.DataFrame(update_data).set_index('id')

        logger.debug(f'Updating {endpoint} data...')
        # Update endpoint dataframe using update_df
        endpoint_df.update(update_df)

        endpoint_df.reset_index(inplace=True)

        endpoint_df.to_csv(f'{igdb_raw_path}igdb_{endpoint}.csv', index=False)
        logger.info(f'COMPLETE: {endpoint} endpoint data successfully updated and written to {igdb_raw_path}igdb_{endpoint}.csv')


def extract_igdb_data_new(connection: IGDBWrapper, config: Dict[str, Any], endpoint: str) -> None:
    """
    Extracts newly created records from a specified IGDB endpoint.
    
    Reads the existing CSV file for an endpoint, identifies the most recent creation
    timestamp, and retrieves only records that have been created since then.
    
    Parameters:
    -----------
    connection : IGDBWrapper
        Authenticated IGDB wrapper instance for making API requests.
    config : Dict[str, Any]
        Configuration dictionary containing data paths with key:
        - 'data': dict with 'igdb_raw_path' for CSV file directory
    endpoint : str
        IGDB API endpoint name to extract new data from.
    
    Returns:
    --------
    None
    """
    logger = get_logger()

    logger.info(f"Beginning IGDB created data retrieval from the {endpoint} endpoint...")

    # Specify parameters
    igdb_raw_path = config['data']['igdb_raw_path']
    csv_path=f'{igdb_raw_path}igdb_{endpoint}.csv'

    # Read endpoint CSV and collect max update and created timestamps
    logger.debug(f'Loading {endpoint}.csv...')
    endpoint_df = pd.read_csv(csv_path, low_memory=False).set_index('id')
    max_create_ts = endpoint_df['created_at'].max()
    query_where = f'where created_at > {max_create_ts};'

    logger.debug(f'Fetching new data from {endpoint} endpoint...')
    # Fetch new data
    new_data = execute_igdb_query(connection=connection, endpoint=endpoint, query_where=query_where)

    # Check for new rows
    if len(new_data) == 0:
        logger.info('No new rows retrieved. File not updated.')
    else:
        logger.info(f'{len(new_data)} new rows successfully retrieved from {endpoint} endpoint')

        new_df = pd.DataFrame(new_data).set_index('id')

        # Find new rows (ids in new_df not in endpoint df)
        new_ids = new_df.index.difference(endpoint_df.index)
        df_insert = new_df.loc[new_ids]

        # Append endpoint df with new records
        new_endpoint_df = pd.concat([endpoint_df, new_df]).reset_index()

        new_endpoint_df.to_csv(f'{igdb_raw_path}igdb_{endpoint}.csv', index=False)
        logger.info(f'COMPLETE: {endpoint} endpoint data successfully updated and written to {igdb_raw_path}igdb_{endpoint}.csv')


def igdb_fuzzy_match_pipeline(config: Dict[str, Any], generate_report: bool = True) -> None:
    """
    Orchestrates the complete fuzzy matching pipeline between library and IGDB data.
    
    Loads library and IGDB data, performs fuzzy matching to identify corresponding
    games, handles deduplication, and optionally generates comprehensive matching reports.
    
    Parameters:
    -----------
    config : Dict[str, Any]
        Configuration dictionary containing data paths with key:
        - 'data': dict with 'processed_path' and 'igdb_raw_path' for file locations
    generate_report : bool, default=True
        Whether to generate detailed matching quality reports.
    
    Returns:
    --------
    None
    """
    logger = get_logger()

    logger.info('Beginning IGDB-Library fuzzy matching...')
    # Load library and igdb data
    library_cleaned=pd.read_csv(f'{config['data']['library_processed_path']}playnite_library.csv')
    igdb_games=pd.read_csv(f'{config['data']['igdb_raw_path']}igdb_games.csv', low_memory=False)

    # Drop igdb_id field from library_cleaned if exists
    library_cleaned.drop(columns='igdb_game_id', inplace=True, errors='ignore')

    # Perform fuzzy matching
    match_df = igdb_library_fuzzy_matching(library_df=library_cleaned, igdb_df=igdb_games, threshold=50)

    # Perform deduplication (library games with multiple IGDB matches)
    library_with_igdb_ids, igdb_data_with_library = filter_and_match_igdb_data(library_df=library_cleaned, igdb_df=igdb_games, match_df=match_df)

    # If True, generate report
    if generate_report:
        igdb_issues_report_path=f'{config['data']['igdb_report_path']}'
        create_comprehensive_igdb_matching_report(igdb_with_library=igdb_data_with_library, library_df=library_cleaned, match_df=match_df, igdb_issues_report_path=igdb_issues_report_path)

    # Append IGDB IDs to playnite_library.csv
    library_with_igdb_ids.to_csv(f'{config['data']['library_processed_path']}playnite_library_igdb.csv', index=False)

    logger.info(
        f"COMPLETE: Library data successfully fuzzy matched with IGDB data and stored in: {config['data']['library_processed_path']}playnite_library_igdb.csv"
    )


def igdb_library_fuzzy_matching(library_df: pd.DataFrame, igdb_df: pd.DataFrame, threshold: int = 50) -> pd.DataFrame:
    """
    Performs fuzzy string matching between library game names and IGDB game names.
    
    Uses optimized fuzzy matching with pre-filtering to find the best matches
    between games in a personal library and the IGDB database based on name similarity.
    
    Parameters:
    -----------
    library_df : pd.DataFrame
        DataFrame containing library games with 'Id' and 'Name' columns.
    igdb_df : pd.DataFrame
        DataFrame containing IGDB games with 'name' column.
    threshold : int, default=50
        Minimum similarity score (0-100) required for a match to be considered valid.
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with columns: 'library_id', 'library_name', 'igdb_name', 'similarity_score'
        containing fuzzy matching results for all library games.
    """
    logger = get_logger()

    # Change library_df index to 'Id' field
    library_df = library_df.set_index('id')
    
    # Get unique game names for fuzzy matching
    igdb_games_unique = igdb_df['name'].dropna().unique().tolist()
    
    logger.debug(f"IGDB has {len(igdb_df)} total entries with {len(igdb_games_unique)} unique game names")
    
    matches = []

    logger.info('Beginning library/IGDB fuzzy matching...')
    for index, row in tqdm(library_df.iterrows(), total=len(library_df)):
        game_name = row['name']
        
        if pd.isna(game_name):
            matches.append({
                'library_id': index,
                'library_name': game_name,
                'igdb_name': None,
                'similarity_score': 0
            })
            continue
        
        # Pre-filter: only consider games that start with same letter
        # This reduces the search space significantly
        first_letter = game_name[0].lower() if game_name else ''
        filtered_candidates = [g for g in igdb_games_unique 
                             if g and g[0].lower() == first_letter]
        
        # If no candidates after filtering, use full list
        if not filtered_candidates:
            filtered_candidates = igdb_games_unique
        
        # Find best match from filtered candidates
        best_match = process.extractOne(
            game_name,
            filtered_candidates,
            scorer=fuzz.ratio
        )
        
        # Check for best match and if score is greater than threshold
        if best_match and best_match[1] >= threshold:
            matches.append({
                'library_id': index,
                'library_name': game_name,
                'igdb_name': best_match[0],
                'similarity_score': best_match[1]
            })
        # Otherwise, assign None to matched_game field
        else:
            matches.append({
                'library_id': index,
                'library_name': game_name,
                'igdb_name': None,
                'similarity_score': best_match[1] if best_match else 0
            })
    
    # Create results dataframe
    match_df = pd.DataFrame(matches)

    logger.info('COMPLETE: Library/IGDB fuzzy matching has completed')

    return match_df


def filter_and_match_igdb_data(library_df: pd.DataFrame, igdb_df: pd.DataFrame, match_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filters duplicate IGDB matches and creates final matched datasets.
    
    Resolves cases where a single library game matches multiple IGDB entries
    by applying selection criteria to choose the best match, then creates
    final datasets with IGDB IDs appended to library data.
    
    Parameters:
    -----------
    library_df : pd.DataFrame
        Original library DataFrame with game information.
    igdb_df : pd.DataFrame
        Complete IGDB games DataFrame.
    match_df : pd.DataFrame
        Fuzzy matching results from igdb_library_fuzzy_matching function.
    
    Returns:
    --------
    Tuple[pd.DataFrame, pd.DataFrame]
        - library_df with 'igdb_game_id' column added
        - merged DataFrame containing both library and IGDB data for matched games
    """
    logger = get_logger()

    logger.debug('Removing duplicate matches from library/IGDB fuzzy matching...')
    # Merge with original library dataframe
    library_df_with_matches = match_df.merge(
        library_df.rename(columns={'id': 'library_id'}),
        on='library_id',
        how='left'
    )

    # Final join with large dataframe - this will create multiple rows per match
    # since the same game name might appear multiple times in igdb_df
    result = library_df_with_matches.merge(
        igdb_df,
        left_on='igdb_name',
        right_on='name',
        how='left',
        suffixes=('_library', '_igdb')
    )

    # Apply deduplication only to rows that have IGDB matches
    matched_rows = result[result['igdb_name'].notna()].copy()
    unmatched_rows = result[result['igdb_name'].isna()].copy()

    if len(matched_rows) > 0:
        # Group by library_id (library record) and apply selection logic
        deduplicated_matches = matched_rows.groupby('library_id')[matched_rows.columns].apply(select_best_igdb_match).reset_index(drop=True)
        
        # Combine deduplicated matches with unmatched rows
        igdb_with_library = pd.concat([deduplicated_matches, unmatched_rows], ignore_index=True)
    else:
        igdb_with_library = result

    # Keep only library and IGDB ids
    id_matches = igdb_with_library[['library_id', 'id']].rename(columns={'library_id': 'id', 'id':'igdb_game_id'})

    # Merge with library_df
    library_df = library_df.merge(id_matches, on='id')

    logger.debug('COMPLETE: Duplicates removed')

    return library_df, igdb_with_library


def select_best_igdb_match(group: pd.DataFrame) -> pd.Series:
    """
    Selects the best IGDB match from multiple candidates for a single library game.
    
    Applies a hierarchical selection process: first by release year proximity,
    then by game category (preferring main games), and finally by data completeness.
    
    Parameters:
    -----------
    group : pd.DataFrame
        DataFrame containing multiple IGDB matches for a single library game.
    
    Returns:
    --------
    pd.Series
        Single row representing the best IGDB match for the library game.
    """
    if len(group) == 1:
        return group.iloc[0]
    
    # First, try to match by release year if available in library data
    library_year = group.iloc[0].get('release_year', None)  # Adjust column name as needed
    if pd.notna(library_year):
        # Convert library year to int for comparison
        try:
            library_year = int(library_year)
            # Filter games with matching or close release years (within 1 year)
            year_matches = group[
                (pd.notna(group['first_release_date'])) &  # Adjust IGDB year column name
                (abs(pd.to_datetime(group['first_release_date']).dt.year - library_year) <= 1)
            ]
            if len(year_matches) > 0:
                group = year_matches
        except (ValueError, TypeError):
            pass  # If year conversion fails, continue with full group
    
    # If still multiple matches, prioritize game_type = 0 (main games)
    if len(group) > 1:
        main_games = group[group['category'] == 0]  # Adjust column name if different
        if len(main_games) > 0:
            group = main_games
    
    # If still multiple matches, select the one with fewest NaN fields
    if len(group) > 1:
        # Count NaN values for each row
        nan_counts = group.isna().sum(axis=1)
        # Select the row with minimum NaN count
        best_idx = nan_counts.idxmin()
        return group.loc[best_idx]
    
    return group.iloc[0]


def create_comprehensive_igdb_matching_report(
    igdb_with_library: pd.DataFrame, 
    library_df: pd.DataFrame, 
    match_df: pd.DataFrame, 
    igdb_issues_report_path: str
) -> None:
    """
    Creates a comprehensive report on the quality of matches between IGDB and library data.
    
    Analyzes matching results to identify potential issues including missing matches,
    low similarity scores, release year mismatches, and game category distributions.
    Generates detailed CSV reports for manual review.
    
    Parameters:
    -----------
    igdb_with_library : pd.DataFrame
        IGDB data merged with library data for analysis.
    library_df : pd.DataFrame
        Original library data used as reference.
    match_df : pd.DataFrame
        DataFrame containing match results from fuzzy matching.
    igdb_issues_report_path : str
        Directory path to save output report files.
    
    Returns:
    --------
    None
    """
    logger = get_logger()

    year_mismatches = []
    no_igdb_records = []
    low_similarity_games = []
    category_analysis = []

    # Get all library games for comparison
    all_library_games = library_df[["id", "name", "release_year"]].copy() if "release_year" in library_df.columns else library_df[["id", "name"]].copy()

    # 1. Check for games with no IGDB records (missing from merged data)
    merged_library_ids = set(igdb_with_library["library_id"].dropna())
    all_library_ids = set(all_library_games["id"])
    missing_library_ids = all_library_ids - merged_library_ids

    for library_id in missing_library_ids:
        game_info = all_library_games[all_library_games["id"] == library_id].iloc[0]
        no_igdb_records.append(
            {
                "library_id": library_id,
                "game_name": game_info["name"],
                "library_year": game_info.get("release_year", "Unknown"),
            }
        )

    # 2. Check for low similarity scores and year mismatches
    for library_id in igdb_with_library["library_id"].unique():
        if pd.isna(library_id):
            continue

        group = igdb_with_library[igdb_with_library["library_id"] == library_id]
        
        if len(group) == 0:
            continue

        game_name = group["library_name"].iloc[0]
        library_year = group.get("release_year", pd.Series([None])).iloc[0] if "release_year" in group.columns else None
        similarity_score = group["similarity_score"].iloc[0] if "similarity_score" in group.columns else None

        # Check for low similarity (games that were matched but with low confidence)
        if pd.notna(similarity_score) and similarity_score < 95:
            igdb_match = group["igdb_name"].iloc[0] if "igdb_name" in group.columns else "Unknown"
            low_similarity_games.append(
                {
                    "library_id": library_id,
                    "game_name": game_name,
                    "library_year": library_year,
                    "similarity_score": similarity_score,
                    "igdb_match": igdb_match,
                }
            )

        # Check for year mismatches (only if both library and IGDB have year data)
        if pd.notna(library_year) and "first_release_date" in group.columns:
            igdb_release_date = group["first_release_date"].iloc[0]
            if pd.notna(igdb_release_date):
                try:
                    # Convert IGDB date to year
                    igdb_year = pd.to_datetime(igdb_release_date).year
                    library_year_int = int(library_year)
                    
                    # Check if years differ by more than 1 year
                    if abs(igdb_year - library_year_int) > 1:
                        year_mismatches.append(
                            {
                                "library_id": library_id,
                                "game_name": game_name,
                                "library_year": library_year_int,
                                "igdb_year": igdb_year,
                                "year_difference": abs(igdb_year - library_year_int),
                                "igdb_match": group["igdb_name"].iloc[0] if "igdb_name" in group.columns else "Unknown",
                            }
                        )
                except (ValueError, TypeError):
                    # Skip if date conversion fails
                    pass

        # Analyze game categories for matched games
        if pd.notna(group["igdb_name"].iloc[0]) and "category" in group.columns:
            category = group["category"].iloc[0]
            category_name = get_igdb_category_name(category)
            category_analysis.append(
                {
                    "library_id": library_id,
                    "game_name": game_name,
                    "igdb_match": group["igdb_name"].iloc[0],
                    "category": category,
                    "category_name": category_name,
                    "similarity_score": similarity_score,
                }
            )

    # Print reports
    logger.info("=" * 80)
    logger.info("COMPREHENSIVE IGDB MATCHING REPORT")
    logger.info("=" * 80)

    # Report 1: Games with no IGDB records
    if no_igdb_records:
        logger.info(
            f"{len(no_igdb_records)} games in library with NO IGDB records found:"
        )
        logger.info("-" * 60)
        no_igdb_df = pd.DataFrame(no_igdb_records)
        no_igdb_df_str = no_igdb_df.to_string(index=False)
        for line in no_igdb_df_str.split('\n'):
            logger.info(line)
        logger.info("-" * 60)
        no_igdb_df.to_csv(f"{igdb_issues_report_path}no_igdb_records.csv", index=False)
        logger.info(f" Details saved to: {igdb_issues_report_path}no_igdb_records.csv")
    else:
        logger.info("All library games have IGDB records!")

    # Report 2: Games with low similarity scores
    if low_similarity_games:
        logger.info(f"{len(low_similarity_games)} games with similarity < 95:")
        logger.info("-" * 60)
        low_sim_df = pd.DataFrame(low_similarity_games)
        low_sim_df_str = low_sim_df.to_string(index=False)
        for line in low_sim_df_str.split('\n'):
            logger.info(line)
        logger.info("-" * 60)
        low_sim_df.to_csv(f"{igdb_issues_report_path}low_similarity_games.csv", index=False)
        logger.info(f" Details saved to: {igdb_issues_report_path}low_similarity_games.csv")
    else:
        logger.info("All matched games have similarity >= 95!")

    # Report 3: Games with year mismatches
    if year_mismatches:
        logger.info(f"{len(year_mismatches)} games with release year mismatches (>1 year difference):")
        logger.info("-" * 60)
        mismatch_df = pd.DataFrame(year_mismatches)
        mismatch_df_str = mismatch_df.to_string(index=False)
        for line in mismatch_df_str.split('\n'):
            logger.info(line)
        logger.info("-" * 60)
        mismatch_df.to_csv(f"{igdb_issues_report_path}year_mismatches.csv", index=False)
        logger.info(f" Details saved to: {igdb_issues_report_path}year_mismatches.csv")
    else:
        logger.info("No significant release year mismatches found!")

    # Report 4: Category analysis for matched games
    if category_analysis:
        logger.info(f"Game category distribution for {len(category_analysis)} matched games:")
        logger.info("-" * 60)
        category_df = pd.DataFrame(category_analysis)
        category_counts = category_df["category_name"].value_counts()
        for category, count in category_counts.items():
            percentage = count / len(category_analysis) * 100
            logger.info(f"   {category}: {count} games ({percentage:.1f}%)")
        
        # Save detailed category analysis
        category_df.to_csv(f"{igdb_issues_report_path}category_analysis.csv", index=False)
        logger.info(f" Detailed category analysis saved to: {igdb_issues_report_path}category_analysis.csv")

        # Check for non-main games
        non_main_games = category_df[category_df["category"] != 0]
        if len(non_main_games) > 0:
            logger.info(f"{len(non_main_games)} matched games are not main games:")
            logger.info("   (Consider reviewing these matches)")
            non_main_summary = non_main_games.groupby("category_name").size()
            for category, count in non_main_summary.items():
                logger.info(f"   - {category}: {count} games")

    # Summary statistics
    total_library_games = len(all_library_games)
    matched_games = len(match_df[match_df["igdb_name"].notna()])
    high_confidence_matches = len(match_df[(match_df["igdb_name"].notna()) & (match_df["similarity_score"] >= 95)])

    logger.info("=" * 80)
    logger.info(" MATCHING SUMMARY:")
    logger.info(f"   Total library games: {total_library_games}")
    logger.info(f"   Games with IGDB matches: {matched_games} ({matched_games/total_library_games*100:.1f}%)")
    logger.info(f"   High confidence matches (>=95): {high_confidence_matches} ({high_confidence_matches/total_library_games*100:.1f}%)")
    logger.info(f"   No IGDB records: {len(no_igdb_records)}")
    logger.info(f"   Low similarity matches: {len(low_similarity_games)}")
    logger.info(f"   Year mismatches: {len(year_mismatches)}")
    
    logger.info("=" * 80)


def get_igdb_category_name(
    category_id: int
) -> str:
    """
    Convert IGDB category ID to human-readable name based on IGDB API documentation.
    
    Parameters:
    -----------
    category_id
        The IGDB category ID to convert to a human-readable name.
    
    Returns:
    --------
    str
        Human-readable category name corresponding to the ID, or "Unknown (ID)" 
        if the category ID is not recognized.
    """
    category_map = {
        0: "Main Game",
        1: "DLC/Addon",
        2: "Expansion",
        3: "Bundle",
        4: "Standalone Expansion",
        5: "Mod",
        6: "Episode",
        7: "Season",
        8: "Remake",
        9: "Remaster",
        10: "Expanded Game",
        11: "Port",
        12: "Fork",
        13: "Pack",
        14: "Update"
    }
    return category_map.get(category_id, f"Unknown ({category_id})")