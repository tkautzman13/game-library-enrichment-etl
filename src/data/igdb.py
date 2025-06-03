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


def test_igdb_connection(connection):
    try:
        connection.api_request(
            'games',
            'fields name; limit 10;'
        )
        print("Success: API test returned results.")
        return True
    except Exception as err:
        print(f"Error occurred during test request: {err}")
        return False
    

def extract_igdb_data(connection, endpoint, fields=['*'], config_path='config.yaml'):
    # Load config file
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Specify parameters
    output_path=config['data']['igdb_raw']
    data = []
    o=0

    print(f'Fetching data from {endpoint} endpoint')
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

    print(f'{len(data)} rows successfully retrieved from {endpoint}')

    df = pd.DataFrame(data)

    df.to_csv(f'{output_path}igdb_{endpoint}.csv', index=False)
    print(f'Complete: {endpoint} data successfully written to {output_path}igdb_{endpoint}.csv')


