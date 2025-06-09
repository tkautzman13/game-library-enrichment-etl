from data.utils import load_config
from data.game_library import extract_library_data, transform_library_data
from data.how_long_to_beat import extract_hltb_data, transform_hltb_data
from data.internet_games_database import connect_to_igdb, extract_and_update_igdb_data, igdb_fuzzy_match_pipeline
from data.playtime_history import extract_playtime_data

def run_data_pipeline(
        library=True,
        hltb=True,
        igdb=True,
        playtime=True
):
    try:
        pipeline_config = load_config('config.yaml')
        print('Beginning data pipeline.')

        if library:
            print("\n" + "=" * 120)
            print(' LIBRARY')
            print("\n" + "=" * 120)
            # Collect game library data and transform
            extract_library_data(config=pipeline_config)
            transform_library_data(config=pipeline_config)

        if hltb:
            print("\n" + "=" * 120)
            print(' HOWLONGTOBEAT')
            print("\n" + "=" * 120)
            # Collect HLTB playtime data
            extract_hltb_data(config=pipeline_config)

            # Process HLTB data
            transform_hltb_data(config=pipeline_config)

        if igdb:
            print("\n" + "=" * 120)
            print(' IGDB')
            print("\n" + "=" * 120)
            # Establish IGDB Connection
            igdb_connection = connect_to_igdb(config=pipeline_config)

            # Extract/Update IGDB Data
            extract_and_update_igdb_data(connection=igdb_connection, config=pipeline_config)

            # Perform fuzzy matching between IGDB and Library data
            igdb_fuzzy_match_pipeline(config=pipeline_config)

        if playtime:
            print("\n" + "=" * 120)
            print(' PLAYTIME')
            print("\n" + "=" * 120)
            # Collect playtime history data
            extract_playtime_data(config=pipeline_config)

        print('Complete: Data pipeline has finished.')
    
    except Exception as e:
        print(f'Pipeline failed: {e}')


if __name__ == '__main__':
    run_data_pipeline()