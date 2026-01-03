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
![game-library-enrichment-pipeline](/docs/diagrams/game-library-enrichment-pipeline.png)

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

### Quick Start
    | Note: Requires Python >= 14.2
    
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

## Output & Results

![game-library-enrichment-erd](/docs/diagrams/game-library-enrichment-erd.png)

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
- **`igdb_collections.csv`** - game collection (aka series)
- **`/hltb_matching_reports/`** - detailed reports on data quality and matching success for HLTB
- **`/igdb_matching_reports/`** - detailed reports on data quality and matching success for IGDB

*NOTE: there are many more endpoints available via the IGDB API that could be added as output datasets. The ones listed above are those that I personally picked that I believed would be relevant to future analytic projects.*

## Challenges Faced
Some of the issues encountered during the development of this pipeline that had to be overcome:
- **Story DLCs/Expansions** - these are rarely treated as standalone games in Playnite, Steam, etc. but usually have their own playtimes and attributes that differ from the main game. In addition, these DLCs/Expansions usually release some time after the main game, meaning players perform a separate 'playthrough' of the DLC months or years after playing the main game. HLTB and IGDB also treat these as separate entities, with their own unique playtimes and attributes. Because of this, I treat story DLCs and Expansions as unique entities within Playnite. They receive their own ID, a "DLC" flag, and their own etries HLTB and IGDB data. Some games - like Mass Effect Legendary Edition - come prepackaged with all of the DLC content. In these cases, the DLCs are not assigned separate entries in Playnite, as the data associated with these games in HLTB and IGDB *assumes* the playtime and game attributes include the DLC.
- **Duplicate games across multiple platforms** - I own some games on multiple platforms (like Xbox and Steam). To remedy this, I tend to use the 'Hide Game' feature in Playnite to hide games on platforms I no longer play on (like Xbox). The pipeline then filters out any games with the 'Hidden' flag set to True.
- **Remasters, Remakes, and Reboots** - Many of these games use the same title as the original (Dead Space from 2008 vs Dead Space from 2023). To prevent multiple matches with HLTB and IGDB records, after matching is performed, the release year associated with the library game is compared to each of the matches from HLTB and IGDB. Only matches with the same release year are kept.
- **Different Name in IGDB vs HLTB** - In some cases, HLTB and IGDB will use different names for the same game. One common example are the Pokemon games; many Pokmeon releases include two version of the game (for example, Red and Blue for the Gameboy), and HLTB combines both of these releases into a single entry ("Pokemon Red & Blue"). On the other hand, IGDB keeps the two releases as separate entries, and also includes "Version" in their names ("Pokemon Blue Version" and "Pokemon Red Version"). These games are flagged in the matching reports as 'low similarity games', and require mitigation efforts to ensure the correct matches are pulled from HLTB and IGDB. These mitigation efforts include manually adjusting a game's name in Playnite or adding specific rules to the Python code prior to matching.
- **"Unending" Games** - Some games - like Mario Party, Call of Duty Warzone, and Minecraft - do not have well-defined playtimes, as they offer generally don't have a "beginning" and "end", but rather are multiplayer or sandbox experiences that are intended to be played for dozens or even hundreds of hours without providing a neat conclusion. These games are not excluded from the HLTB playtime collection process, but should be treated carefully in any playtime prediction projects.

## What's Next

This ETL pipeline creates the foundation for several downstream projects:

- **Playtime Prediction Models**: ML models to predict personal game completion times
- **Integration with Other Gaming ETL Pipelines**: Connect enriched library data with other datasets such as personal playtime history, game reflections, game screenshots and other datasets to allow for the following projects:
    - **Daily Playtime Forecasting**: Forecast future daily playtimes
    - **Game Reflection NLP Projects**: Further enhance knowledge of personal gaming preferences
    - **Game Screenshot Computer Vision Projects**: Cluster games with similar artistic design, identify characters and UI elements on screen, etc.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
