import yaml
from data.game_library import extract_library_data, transform_library_data
from data.how_long_to_beat import extract_hltb_data, transform_hltb_data
from data.igdb import connect_to_igdb, extract_and_update_igdb_data, igdb_fuzzy_match_pipeline

# Config file path
config_path = 'config.yaml'

# Load config file
with open(config_path, "r") as f:
    pipeline_config = yaml.safe_load(f)

# Collect game library data
extract_library_data(config=pipeline_config)
transform_library_data(config=pipeline_config)

# Collect HLTB playtime data
extract_hltb_data(config=pipeline_config)

# Process HLTB data
transform_hltb_data(config=pipeline_config)

# Establish IGDB Connection
igdb_connection = connect_to_igdb(config=pipeline_config)

# Extract/Update IGDB Data
extract_and_update_igdb_data(connection=igdb_connection, config=pipeline_config)

# Perform fuzzy matching between IGDB and Library data
igdb_fuzzy_match_pipeline(config=pipeline_config)