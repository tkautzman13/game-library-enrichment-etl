import os
import requests
import yaml
import pandas as pd
import json
from igdb.wrapper import IGDBWrapper
import time

def connect_to_igdb(config):
    print('Beginning IGDB connection...')

    # Credentials for url
    client_id = config['igdb_api']['client_id']
    client_secret = config['igdb_api']['client_secret']
    grant_type = 'client_credentials'

    # POST request
    url = f'https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type={grant_type}'
    response = requests.post(url)
    if response.status_code == 200:
        # Get access token
        token = response.json().get('access_token')
        print('Connection successful, accesss token received.')
        connection = IGDBWrapper(client_id, token)
        return connection
    else:
        print(f"Failed to get token: {response.status_code}")
        print(response.text)


def test_igdb_connection(connection):
    print('Beginning IGDB connection test...')
    try:
        connection.api_request(
            'games',
            'fields name; limit 10;'
        )
        print('Connection test succeeded!')
        return True
    except Exception as err:
        print(f"Error occurred during test request: {err}")
        return False


def extract_and_update_igdb_data(connection, config):
    print('Beginning IGDB extracts/updates...')

    # Specify parameters
    igdb_raw_path = config['data']['igdb_raw_path']
    endpoint_list = [
        'games', 'franchises', 'game_types', 'genres', 'themes', 'keywords', 'player_perspectives'
    ]

    # Test IGDB Connection before running
    if test_igdb_connection(connection):
        for endpoint in endpoint_list:
            if os.path.isfile(f'{igdb_raw_path}igdb_{endpoint}.csv'):
                print(f'{endpoint}.csv already exists. Updating...')
            else:
                extract_igdb_data(connection=connection, endpoint=endpoint, config=config)
                # Pause for 1 second to avoid too many requests
                time.sleep(1)

    print('Complete: IGDB data successfully updated/extracted.')


def extract_igdb_data(connection, endpoint, config, fields=['*']):
    print(f"Beginning IGDB data extraction from the {endpoint} endpoint...")

    # Specify parameters
    output_path=config['data']['igdb_raw_path']
    data = []
    o=0

    print(f'Fetching data from {endpoint} endpoint...')
    # Fetch games data
    while True:
        json_results = connection.api_request(
            f'{endpoint}',
            f'fields {', '.join(fields)}; limit 500; offset {o};'
        )

        json_load = json.loads(json_results)

        data.extend(json_load)

        o += 500

        if len(json_load) < 500:
            break

    print(f'{len(data)} rows successfully retrieved from {endpoint}.')

    df = pd.DataFrame(data)

    df.to_csv(f'{output_path}igdb_{endpoint}.csv', index=False)
    print(f'Complete: {endpoint} data successfully written to {output_path}igdb_{endpoint}.csv')

