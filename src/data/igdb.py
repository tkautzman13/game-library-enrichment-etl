import os
import requests
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


def execute_igdb_query(connection, endpoint, query_where=None):
    o=0
    data=[]
    
    # Fetch data
    while True:
        if query_where:
            json_results = connection.api_request(
                f'{endpoint}',
                f'fields *; limit 500; offset {o};{query_where}'
            )
        else:
            json_results = connection.api_request(
                f'{endpoint}',
                f'fields *; limit 500; offset {o};'
            )            

        json_load = json.loads(json_results)

        data.extend(json_load)

        o += 500

        if len(json_load) < 500:
            break

    return data


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
            print("\n" + "=" * 60)
            print(f"    {endpoint}")
            print("=" * 60)
            if os.path.isfile(f'{igdb_raw_path}igdb_{endpoint}.csv'):
                print(f'{endpoint}.csv already exists. Collecting updates and new records...')
                extract_igdb_data_new(connection=connection, endpoint=endpoint, config=config)
                time.sleep(0.5)
                update_igdb_data(connection=connection, endpoint=endpoint, config=config)
                time.sleep(0.5)
            else:
                print(f'{endpoint}.csv does not exist. Performing full data load...')
                extract_igdb_data_full(connection=connection, endpoint=endpoint, config=config)
                # Pause for 1 second to avoid too many requests
                time.sleep(0.5)

    print('Complete: IGDB data successfully updated/extracted.')


def extract_igdb_data_full(connection, endpoint, config):
    print(f"Beginning IGDB data extraction from the {endpoint} endpoint...")

    # Specify parameters
    output_path=config['data']['igdb_raw_path']

    print(f'Fetching data from {endpoint} endpoint...')
    # Fetch games data
    data = execute_igdb_query(connection=connection, endpoint=endpoint)

    print(f'{len(data)} rows successfully retrieved from {endpoint}.')

    df = pd.DataFrame(data)

    df.to_csv(f'{output_path}igdb_{endpoint}.csv', index=False)
    print(f'Complete: {endpoint} data successfully written to {output_path}igdb_{endpoint}.csv')


def update_igdb_data(connection, config, endpoint):
    print(f"Beginning IGDB updated data retrieval from the {endpoint} endpoint...")

    # Specify parameters
    igdb_raw_path = config['data']['igdb_raw_path']
    csv_path=f'{igdb_raw_path}igdb_{endpoint}.csv'

    # Read endpoint CSV and collect max update and created timestamps
    print(f'Loading {endpoint}.csv...')
    endpoint_df = pd.read_csv(csv_path, low_memory=False).set_index('id')
    max_update_ts = endpoint_df['updated_at'].max()
    query_where = f'where updated_at > {max_update_ts};'

    print(f'Fetching updates from {endpoint} endpoint...')
    # Fetch updated data
    update_data = execute_igdb_query(connection=connection, endpoint=endpoint, query_where=query_where)

    # Check for new rows
    if len(update_data) == 0:
        print('No updated rows retrieved. File not updated.')
    else:
        print(f'{len(update_data)} updated rows successfully retrieved from {endpoint}.')

        # Setup new dataframes
        update_df = pd.DataFrame(update_data).set_index('id')

        print(f'Updating {endpoint} data...')
        # Update endpoint dataframe using update_df
        endpoint_df.update(update_df)

        endpoint_df.reset_index(inplace=True)

        endpoint_df.to_csv(f'{igdb_raw_path}igdb_{endpoint}.csv', index=False)
        print(f'Complete: {endpoint} data successfully updated and written to {igdb_raw_path}igdb_{endpoint}.csv')


def extract_igdb_data_new(connection, config, endpoint):
    print(f"Beginning IGDB created data retrieval from the {endpoint} endpoint...")

    # Specify parameters
    igdb_raw_path = config['data']['igdb_raw_path']
    csv_path=f'{igdb_raw_path}igdb_{endpoint}.csv'

    # Read endpoint CSV and collect max update and created timestamps
    print(f'Loading {endpoint}.csv...')
    endpoint_df = pd.read_csv(csv_path, low_memory=False).set_index('id')
    max_create_ts = endpoint_df['created_at'].max()
    query_where = f'where created_at > {max_create_ts};'

    print(f'Fetching new data from {endpoint} endpoint...')
    # Fetch new data
    new_data = execute_igdb_query(connection=connection, endpoint=endpoint, query_where=query_where)

    # Check for new rows
    if len(new_data) == 0:
        print('No new rows retrieved. File not updated.')
    else:
        print(f'{len(new_data)} new rows successfully retrieved from {endpoint}.')

        new_df = pd.DataFrame(new_data).set_index('id')

        # Find new rows (ids in new_df not in endpoint df)
        new_ids = new_df.index.difference(endpoint_df.index)
        df_insert = new_df.loc[new_ids]

        # Append endpoint df with new records
        new_endpoint_df = pd.concat([endpoint_df, new_df]).reset_index()

        new_endpoint_df.to_csv(f'{igdb_raw_path}igdb_{endpoint}.csv', index=True)
        print(f'Complete: {endpoint} data successfully updated and written to {igdb_raw_path}igdb_{endpoint}.csv')