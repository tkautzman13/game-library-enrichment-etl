from data.game_library import copy_raw_library_data, clean_library_data
from data.how_long_to_beat import prepare_library_data_hltb, extract_raw_hltb_data, process_hltb_data


# Collect game library data
copy_raw_library_data()
clean_library_data()

# Prepare library data for HLTB data collection
prepare_library_data_hltb()

# Collect HLTB playtime data
extract_raw_hltb_data()

# Process HLTB + Library data
process_hltb_data()