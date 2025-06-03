from data.game_library import extract_library_data, transform_library_data
from data.how_long_to_beat import transform_library_data_for_hltb, extract_hltb_data, transform_hltb_data
from data.igdb import connect_to_igdb, extract_and_update_igdb_data

# Config file path
config_path = 'config.yaml'

# Collect game library data
extract_library_data()
transform_library_data()

# Prepare library data for HLTB data collection -- abstract this function into extract_hltb_data()
transform_library_data_for_hltb()

# Collect HLTB playtime data
extract_hltb_data()

# Process HLTB data
transform_hltb_data()

# Establish IGDB Connection
igdb_connection = connect_to_igdb()

# Extract/Update IGDB Data
extract_and_update_igdb_data(connection=igdb_connection)