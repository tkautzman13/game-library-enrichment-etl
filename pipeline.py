from src.utils import load_config, parse_args, setup_logger, ensure_directories_exist
from src.game_library import extract_library_data, transform_library_data
from src.how_long_to_beat import extract_hltb_data, transform_hltb_data
from src.internet_games_database import connect_to_igdb, extract_and_update_igdb_data, igdb_fuzzy_match_pipeline, copy_igdb_data_to_processed

def run_data_pipeline(
        library=True,
        hltb=True,
        igdb=True,
        skip_igdb_api=False,
        config_file='config.yaml'
):
    logger = setup_logger()
    logger.info('Beginning data pipeline')

    try:
        # Load config file
        pipeline_config = load_config(config_file)

        # Ensure directories found in config exist
        directories = [
            value for key, value in pipeline_config['data'].items()
            if isinstance(value, str) and value.endswith('/')
        ]

        ensure_directories_exist(directories)

        if library:
            logger.info("=" * 120)
            logger.info(' LIBRARY')
            logger.info("=" * 120)
            # Collect game library data and transform
            extract_library_data(config=pipeline_config)
            transform_library_data(config=pipeline_config)

        if hltb:
            logger.info("=" * 120)
            logger.info(' HOWLONGTOBEAT')
            logger.info("=" * 120)
            # Collect HLTB playtime data
            extract_hltb_data(config=pipeline_config)

            # Process HLTB data
            transform_hltb_data(config=pipeline_config)

        if igdb:
            logger.info("=" * 120)
            logger.info(' IGDB')
            logger.info("=" * 120)
            if not skip_igdb_api:
                # Establish IGDB Connection
                igdb_connection = connect_to_igdb(config=pipeline_config)

                # Extract/Update IGDB Data
                extract_and_update_igdb_data(connection=igdb_connection, config=pipeline_config)

            # Perform fuzzy matching between IGDB and Library data
            igdb_fuzzy_match_pipeline(config=pipeline_config)

            # Copy igdb data to processed
            copy_igdb_data_to_processed(config=pipeline_config)
            

        logger.info('COMPLETE: Data pipeline has finished')
    
    except Exception as e:
        logger.exception('Pipeline failed with error')


if __name__ == '__main__':
    args = parse_args()
    
    run_data_pipeline(
        library=args.library,
        hltb=args.hltb,
        igdb=args.igdb,
        skip_igdb_api=args.skip_igdb_api,
        config_file=args.config
    )