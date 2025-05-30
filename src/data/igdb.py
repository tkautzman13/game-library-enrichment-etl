import requests
import yaml
import pandas as pd
import json
from igdb.wrapper import IGDBWrapper

def connect_to_igdb(config_path='config.yaml'):
    # Load config file
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

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
        print('Token received.')
        connection = IGDBWrapper(client_id, token)
        return connection
    else:
        print(f"Failed to get token: {response.status_code}")
        print(response.text)


def test_connection(connection):
    try:
        json_results = connection.api_request(
            'games',
            'fields name; limit 10;'
        )
        print("Success: test API request returned results.")
        return json_results
    except Exception as err:
        print(f"Error occurred during test request: {err}")