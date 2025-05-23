from data.game_library import copy_raw_library_data, process_library_data
from data.how_long_to_beat import hltb_prepare_library_data, extract_raw_hltb_data


# Collect game library data
copy_raw_library_data()
process_library_data()

# Prepare library data for HLTB data collection
library_prepped = hltb_prepare_library_data()

# Collect HLTB playtime data
extract_raw_hltb_data(library_prepped=library_prepped)
