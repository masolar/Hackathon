import os
import psycopg2
from argparse import ArgumentParser
import json
from tqdm import tqdm
from typing import Dict, Any

def parse_args(parser: ArgumentParser):
    """
    Parses arguments for the data building script
    """
    parser.add_argument('data_folder', type=str, help='The folder that holds the data for the database')

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args(ArgumentParser())

    conn = psycopg2.connect(
        host="localhost",
        database="hackathon",
        user=os.environ['DB_USERNAME'],
        password=os.environ['DB_PASSWORD'])

    # Open a cursor to perform database operations
    cur = conn.cursor()

    cur.execute('DROP TABLE IF EXISTS fhir;')
    cur.execute('CREATE TABLE fhir (id serial NOT NULL PRIMARY KEY,'
                                    'data json NOT NULL);'
               )
    conn.commit()
    
    for filename in tqdm(os.listdir(args.data_folder)):
        with open(os.path.join(args.data_folder, filename)) as f:
            data = json.load(f) # Get the json from the file into a Python file
            test = json.dumps(data) # Convert back to JSON. Makes sure it's formatted correctly
            cur.execute(f'INSERT INTO fhir (data) VALUES (%s)', (test,)) # Have to give a tuple, even in this case
    conn.commit()
