import argparse
from data.utils import load_config
from data.game_library import extract_library_data, transform_library_data
from data.how_long_to_beat import extract_hltb_data, transform_hltb_data
from data.internet_games_database import connect_to_igdb, extract_and_update_igdb_data, igdb_fuzzy_match_pipeline
from data.playtime_history import extract_playtime_data

def run_data_pipeline(
        library=True,
        hltb=True,
        igdb=True,
        playtime=True,
        config_file='config.yaml'
):
    try:
        pipeline_config = load_config(config_file)
        print('Beginning data pipeline.')

        if library:
            print("\n" + "=" * 120)
            print(' LIBRARY')
            print("=" * 120)
            # Collect game library data and transform
            extract_library_data(config=pipeline_config)
            transform_library_data(config=pipeline_config)

        if hltb:
            print("\n" + "=" * 120)
            print(' HOWLONGTOBEAT')
            print("=" * 120)
            # Collect HLTB playtime data
            extract_hltb_data(config=pipeline_config)

            # Process HLTB data
            transform_hltb_data(config=pipeline_config)

        if igdb:
            print("\n" + "=" * 120)
            print(' IGDB')
            print("=" * 120)
            # Establish IGDB Connection
            igdb_connection = connect_to_igdb(config=pipeline_config)

            # Extract/Update IGDB Data
            extract_and_update_igdb_data(connection=igdb_connection, config=pipeline_config)

            # Perform fuzzy matching between IGDB and Library data
            igdb_fuzzy_match_pipeline(config=pipeline_config)

        if playtime:
            print("\n" + "=" * 120)
            print(' PLAYTIME')
            print("=" * 120)
            # Collect playtime history data
            extract_playtime_data(config=pipeline_config)

        print('Complete: Data pipeline has finished.')
    
    except Exception as e:
        print(f'Pipeline failed: {e}')


def parse_args():
    parser = argparse.ArgumentParser(description='Run the data pipeline with configurable components')
    
    # Add arguments for each pipeline component
    parser.add_argument('--library', action='store_true', default=False,
                       help='Run the library data pipeline (default: False)')
    parser.add_argument('--hltb', action='store_true', default=False,
                       help='Run the HowLongToBeat data pipeline (default: False)')
    parser.add_argument('--igdb', action='store_true', default=False,
                       help='Run the IGDB data pipeline (default: False)')
    parser.add_argument('--playtime', action='store_true', default=False,
                       help='Run the playtime history pipeline (default: False)')
    
    # Special flag to run all components (maintains backward compatibility)
    parser.add_argument('--all', action='store_true', default=False,
                       help='Run all pipeline components')
    
    # Config file argument
    parser.add_argument('--config', type=str, default='config.yaml',
                       help='Path to config file (default: config.yaml)')
    
    args = parser.parse_args()
    
    # If --all is specified, set all components to True
    if args.all:
        args.library = True
        args.hltb = True
        args.igdb = True
        args.playtime = True
    
    # If no specific components are selected and --all is not used, run all by default
    if not any([args.library, args.hltb, args.igdb, args.playtime]):
        args.library = True
        args.hltb = True
        args.igdb = True
        args.playtime = True
    
    return args


if __name__ == '__main__':
    args = parse_args()
    
    run_data_pipeline(
        library=args.library,
        hltb=args.hltb,
        igdb=args.igdb,
        playtime=args.playtime,
        config_file=args.config
    )