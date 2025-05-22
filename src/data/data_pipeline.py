from data.game_library import collect_library_raw_data, process_library_data
import yaml

# Pull in yaml config file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Collect Game Library Data
collect_library_raw_data(input_path=config["data"]["library_input_path"], output_path=config["data"]["library_raw_path"])
process_library_data(input_path=config["data"]["library_raw_path"], output_path=config["data"]["library_processed_path"])