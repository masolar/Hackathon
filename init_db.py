import os
import psycopg2
from argparse import ArgumentParser
import json
from tqdm import tqdm
from typing import Dict, Any, List
import multiprocessing

def parse_args(parser: ArgumentParser):
    """
    Parses arguments for the data building script
    """
    parser.add_argument('data_folder', type=str, help='The folder that holds the data for the database')
    parser.add_argument('--import_data', action='store_true', help='Whether or not to import the data into the dataset. Will take a while.')
    parser.add_argument('--num_processes', '-p', type=int, default=4, help='The number of processes for loading in data to the data table')

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args(ArgumentParser())
    
    conn_info = {'host':"localhost",
                 'database':"hackathon",
                 'user':os.environ['DB_USERNAME'],
                 'password':os.environ['DB_PASSWORD']}
    conn = psycopg2.connect(**conn_info)

    # Open a cursor to perform database operations
    cur = conn.cursor()
    
    # Setup the users table
    cur.execute('DROP TABLE IF EXISTS users CASCADE;')
    cur.execute('CREATE TABLE users (_id serial NOT NULL PRIMARY KEY,'
                                     'email varchar not null,'
                                     'password varchar not null,'
                                     'first_name varchar not null,'
                                     'last_name varchar not null,'
                                     'own_docs json,'
                                     'shared_docs json);')
    
    # Setup the saved file information
    cur.execute('DROP TABLE IF EXISTS docs;') 
    cur.execute('CREATE TABLE docs (doc_id serial NOT NULL PRIMARY KEY,' 
                                    'user_id serial NOT NULL,'
                                    'file_link varchar NOT NULL,'
                                    'preview_link varchar NOT NULL,'
                                    'CONSTRAINT fk_user '
                                    'FOREIGN KEY (user_id) '
                                    'REFERENCES users(_id));')
    conn.commit()
    
    # Setup the fhir data tables and import data, which may take a while
    if args.import_data:
        cur.execute('DROP TABLE IF EXISTS fhir;')
        cur.execute('CREATE TABLE fhir (id serial NOT NULL PRIMARY KEY,'
                                        'data json NOT NULL);'
                   )

        conn.commit()

        filepaths = list(map(lambda x: os.path.join(args.data_folder, x), filter(lambda x: '.DS_Store' not in x, os.listdir(args.data_folder))))

        
        # Build up arguments for starmap
        for filepath in tqdm(filepaths):
            with open(filepath, 'r') as f:
                data = json.load(f) # Get the json from the file into a Python file
                test = json.dumps(data) # Convert back to JSON. Makes sure it's formatted correctly
                cur.execute(f'INSERT INTO fhir (data) VALUES (%s)', (test,)) # Have to give a tuple, even in this case
        conn.commit()
