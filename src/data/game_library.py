import pandas as pd

def collect_library_raw_data(input_path, output_path):
    library_raw = pd.read_csv(input_path)
    library_raw.to_csv(output_path, index = False)

    print(f'Library data successfully pulled from {input_path} and stored in: {output_path}')


def process_library_data(input_path,output_path):
    library_interm = pd.read_csv(input_path)

    # Remove (Xbox), (Game Pass), (Switch), (PlayStation) tags from game names
    tags = {' (Xbox)', ' (Game Pass)', ' (Switch)', ' (PlayStation)'}
    replacement = ''

    for tag in tags:
        library_interm['Name'] = library_interm['Name'].str.replace(tag, replacement)

    # No 'Apps' Category games (ie GamePass)
    library_interm[library_interm['Completion Status'] != 'Apps']

    # No games without a library Completion Status
    library_interm = library_interm[~library_interm['Completion Status'].isnull()]

    # Drop duplicates
    library_interm = library_interm.drop_duplicates(subset=['Name', 'Release Date'])

    # Export intermediate data
    library_interm.to_csv(output_path, index=False)

    print(f'Library data successfully processed and stored in: {output_path}')