from data.game_library import extract_library_data, transform_library_data
from data.how_long_to_beat import transform_library_data_for_hltb, extract_hltb_data, transform_hltb_data

# Config file path
config_path = 'config.yaml'

# Collect game library data
extract_library_data()
transform_library_data()

# Prepare library data for HLTB data collection
transform_library_data_for_hltb()

# Collect HLTB playtime data
extract_hltb_data()

# Process HLTB data
transform_hltb_data()