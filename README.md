# Video Game Library Enrichment ETL Pipeline
A Python data pipeline that enriches my personal video game library data with external sources, creating datasets ready for further analytic and machine learning projects.

## Project Overview

### Purpose
The purpose of this ETL pipeline is to automatically build out a personal video game analytics data model based on the games available in my personal library (managed by the Playnite application).

### What it Does:
- **Generates and Extracts** game data from the following sources:
    - **[Playnite](https://playnite.link/)** - a universal game library manager
    - **[HowLongToBeat (HLTB)](https://howlongtobeat.com/)** - a community-driven website containing aggregated playtimes for thousands of video games
    - **[Internet Games Database (IGDB)](https://www.igdb.com/)** - an online database containing comprehensive information about thousands of video games
- **Connects** the three data sources together through search and fuzzy matching
- **Reports** on matching quality and unmatched entries for both HLTB and IGDB matches
- **Outputs** a clean, ready-to-consume data model for use in future analytic projects


## Architecture & Data Flow

### Pipeline:
![game-library-enrichment-pipeline](https://github.com/user-attachments/assets/50d7e008-70f8-40ce-8f74-1ab0060b15ee)

### Core ETL Components
- **`playnite_library_extract.ps1`** - PowerShell script embedded within the Playnite application that exports library data
    - To set this up, in Playnite, go to Settings -> Scripts and then add the code found within this script to one of the text boxes (I place it in the 'Execute on application start' option)
    - I also set up a Task Scheduler task to automatically restart Playnite everyday in order to execute this script. This code is found in the **`playnite_restart.ps1`** script
- **`pipeline.py`** - Main orchestrator that coordinates all ETL processes
- **`game_library.py`** - Extracts and cleans Playnite library exports
- **`how_long_to_beat.py`** - Utilizes the [howlongtobeatpy](https://pypi.org/project/howlongtobeatpy/) Python API to search and match library games with HLTB games to extract community playtime data
- **`internet_games_database.py`** - Retrieves datasets from [IGDB API](https://www.igdb.com/api) endpoints, and performs fuzzy matching with library data to match games
- **`utils.py`** - Utilities script for configuration, logging, and CLI handling

## Key Technical Features

### Smart Data Matching
- **Fuzzy matching algorithms** to connect games across different databases
- **Comprehensive reporting** on match quality and unmatched entries

### Flexible & Reliable Processing
- **Modular design** - ability to run the full pipeline or individual components
- **Configuration-driven** - easily switch input and output files and paths
- **Detailed logging** - track progress and troubleshoot issues

### Automation Ready
- **CLI interface** - designed for scheduled execution
- **Sample data setup available** - for testing without API credentials
- **Task Scheduler compatible** - for automated daily/weekly runs

### Prerequisites
- Python 3.12+
- *Optional: IGDB API credentials (free from Twitch Developer Portal)*
    - *Sample file exists to skip IGDB API credentials (see configuration setup below)*
- *Optional: Playnite installation for full functionality*
    - *NOTE: without Playnite, many manual steps will be required to setup library data extraction for pipeline*

### Installation
```bash
git clone https://github.com/tkautzman13/game-library-enrichment-etl.git
cd game-library-enrichment-etl
pip install -r requirements.txt
```

### Configuration
**If you don't want to setup IGDB API credentials and just want to run the pipeline using the sample data**: 
- Skip this section and go straight to [Quick Start](#Quick-Start).

**If you wish to run your own library through the pipeline *and* you have setup IGDB API credentials**:
1. Copy `sample.yaml` to `config.yaml`
2. Update your IGDB API credentials (if you want to pull all IGDB data):
```yaml
igdb_api:
  client_id: "your_client_id"
  client_secret: "your_client_secret"
```
3. The project will output CSVs for the data model within the project data folder by default. If you wish to change this, adjust the following variables:
```yaml
data:
  library_source_file: "data/library_sample_data.csv" # Change to your own library file path if needed
  library_raw_path: "data/raw/library/"
  library_processed_path: "data/processed/library/"

  hltb_raw_path: "data/raw/hltb/extracts/"
  hltb_processed_path: "data/processed/hltb/"
  hltb_report_path: "data/processed/hltb/reports/"

  igdb_raw_path: "data/sample/" # Change to "data/raw/igdb/" if using API credentials
  igdb_processed_path: "data/processed/igdb/"
  igdb_report_path: "data/processed/igdb/reports/"
```

### Quick Start
To run the pipeline, see below CLI prompts:

```bash
# Test with provided sample data (no API credentials or library data needed)
python pipeline.py --config sample.yaml --skip_igdb_api 

# Run full enrichment pipeline using config.yaml (config.yaml must contain igdb credentials)
python pipeline.py

# Run specific components only
python pipeline.py --config config.yaml --library
python pipeline.py --config config.yaml --hltb
python pipeline.py --config config.yaml --library --igdb
python pipeline.py --config config.yaml --library --skip_igdb_api
```

### Using Your Own Library
If you want to use this script with your own game library, **it is highly recommended you use Playnite** to extract your library data using the code found in the `playnite_library_extract.ps1` script. The pipeline expects the library to be formatted a certain way, and will require edits if the library dataset format is different. You also may want to adjust the `transform_library_data()` function in `game_library.py` to remove any specific filtering and cleaning steps you don't want/need. Currently, the steps present in this function are for my own personal use case*

To set up the `playnite_library_extract.ps1` script to extract your Playnite library data: 
1. In Playnite, go to Settings -> Scripts
2. Add the code from `playnite_library_extract.ps1` to one of the script text boxes (I place it in the 'Execute on application start' option)
3. If you placed the code in the 'Execute on application start' or 'Execute on application shutdown', you may want to set up a Task Scheduler task or CRON job to automatically restart Playnite on a regular basis to execute the script. The code found in the `playnite_restart.ps1` script is what I used to set this up

## Output & Results

![game-library-enrichment-erd](https://github.com/user-attachments/assets/0b4106e6-7b09-4c95-8ab3-36bc4076247d)

### Generated Datasets
- **`playnite_library_igdb.csv`** - cleaned library with IGDB game IDs
- **`hltb_playtimes.csv`** - HowLongToBeat community playtime data
- **`igdb_games.csv`** - main IGDB games table
- **`igdb_game_types.csv`** - game types (main game, expansion, DLC, etc.)
- **`igdb_genres.csv`** - game genres (Point-and-click, Fighting, Shooter, etc.)
- **`igdb_franchises.csv`** - game franchises (Halo, Call of Duty, Madden, etc.)
- **`igdb_keywords.csv`** - words or phrases that get tagged to a game ('World War 2', 'Steampunk', etc.)
- **`igdb_themes.csv`** - game theme (Drama, Non-fiction, Sandbox, etc.)
- **`igdb_player_perspectives.csv`** - describes the view/perspective of the player in a game (First person, Third Person, Text, etc.)
- **`/hltb_matching_reports/`** - detailed reports on data quality and matching success for HLTB
- **`/igdb_matching_reports/`** - detailed reports on data quality and matching success for IGDB

*NOTE: there are many more endpoints available via the IGDB API that could be added as output datasets. The ones listed above are those that I personally picked that I believed would be relevant to future analytic projects.*

## Challenges Faced
Some of the issues encountered during the development of this pipeline that had to be overcome:
- **Story DLCs/Expansions** - these are rarely treated as standalone games in Playnite, Steam, etc. but usually have their own playtimes and attributes that differ from the main game. In addition, these DLCs/Expansions usually release some time after the main game, meaning players usually do a separate 'playthrough' of the DLC months or years after playing the main game. HLTB and IGDB also treat these as separate entities, with their own unique playtimes and attributes. Because of this, I treat story DLCs and Expansions as unique entities within Playnite. They receive their own ID, a "DLC" category within Playnite, and their own ties to HLTB and IGDB data. Some games - like Mass Effect Legendary Edition - come prepackaged with all of the DLC content. In these cases, the DLCs do not receive separate entries in Playnite, as the data associated with these games in HLTB and IGDB *assumes* the playtime and game attributes include the DLC.
- **Duplicate games across multiple platforms** - I own some games on multiple platforms (like Xbox and Steam). To remedy this, I tend to use the 'Hide Game' feature in Playnite to hide games on platforms I no longer play on (like Xbox) when I have the game available on my PC. The pipeline then filters out any games with the 'Hidden' flag set to True.
- **Remasters, Remakes, and Reboots** - many of these games use the exact same name as the original (Dead Space from 2008 vs Dead Space from 2023). To prevent multiple exact matches in HLTB and IGDB, after matching is performed, the release year associated with the Playnite library game is compared to each of the matches in HLTB and IGDB. Only matches with the same release year are kept.
- **Different Name in IGDB vs HLTB** - in some cases, HLTB and IGDB will use different names for the same game. One common example are the Pokemon games; many Pokmeon releases include two version of the game (for example, Red and Blue for the Gameboy), and HLTB combines both of these releases into a single entry ("Pokemon Red & Blue"). On the other hand, IGDB keeps the two releases as separate entries, and also includes "Version" in the names ("Pokemon Blue Version" and "Pokemon Red Version"). These games are captured in the matching reports as 'low similarity games', and require mitigation methods to ensure the correct matches are being pulled from HLTB and IGDB. These mitigation methods could include manually adjusting a game's name in Playnite or adding specific rules to the Python code prior to matching with HLTB and IGDB records.
- **"Unending" Games** - some games - like Mario Party, Call of Duty Warzone, and Minecraft - do not have well-defined playtimes, as they offer generally don't have a "beginning" and "end", but rather are multiplayer or sandbox experiences that are intended to be played for dozens or even hundreds of hours without providing a neat conclusion. These games are not excluded from the HLTB playtime collection process, but should be treated carefully in any playtime prediction projects.

## What's Next

This ETL pipeline creates the foundation for several downstream projects:

- **Playtime Prediction Models**: ML models to predict personal game completion times
- **Integration with Other Gaming ETL Pipelines**: Connect enriched library data with other datasets such as personal playtime history, game reflections, game screenshots and other datasets to allow for the following projects:
    - **Daily Playtime Forecasting**: Forecast future daily playtimes
    - **Game Reflection NLP Projects**: Further enhance knowledge of personal gaming preferences
    - **Game Screenshot Computer Vision Projects**: Cluster games with similar artistic design, identify characters and UI elements on screen, etc.

In addition, I'm planning on adding more functionalities to the ETL pipeline in the future (most of these additions are meant to help me build out my skills in the following areas):
- [ ] Alerting and monitoring to inform the user whenever the pipeline fails
- [ ] Unit testing
- [ ] Data validation and testing

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.