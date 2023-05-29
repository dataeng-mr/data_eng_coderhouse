import requests
from datetime import datetime
import hashlib
import redshift_connector
from sqlalchemy import create_engine

class MarvelClient():
    """
    Handles Marvel API interactions.
    """    
    def __init__(self, url, public_key, private_key):
        self.marvel_url = url.strip('/') + '/'
        self.ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.public_key = public_key
        self.private_key = private_key
    
    def md5_digest(self):
        str2hash = f'{self.ts}{self.private_key}{self.public_key}'
        result = hashlib.md5(str2hash.encode())
        hex_hash = result.hexdigest()
        return hex_hash
    
    def get_request_params(self):
        params = {
        'ts': self.ts,
        'apikey': self.public_key,
        'hash': self.md5_digest()}
        return params
    
    def get_characters_number(self):
        """Route: GET public/characters"""
        route = self.marvel_url + 'public/characters'
        characters_numbers = requests.get(route, params=self.get_request_params()).json().get('data').get('total')                  
        return characters_numbers

    def get_characters_list(self):
        """Route: GET public/characters"""
        route = self.marvel_url + 'public/characters'
        params = self.get_request_params()
        limit = self.get_characters_number()
        if limit > 100:
            limit = 100
        params['limit'] = limit
        characters = requests.get(route, params=params).json().get('data').get('results')
        characters_list = []
        for character in characters:
            characters_list.append(dict({
                'id': character.get('id'),
                'name': character.get('name'),
                'description': character.get('description'),
                'modified': character.get('modified')
            }))
        return characters_list
    
def redshift_connection(host, port, database, user, password):
    try:
        print('Connecting to Redshift Cluster')
        conn = redshift_connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password)
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
        print('Succesfully connected to Redshift')
        return conn, engine
    except ConnectionError as e:
        print('Failed to connect to redshift, please review your access credentials')
        raise e
    
def exec_sql_query(conn, query):
    try:
        cursor = conn.cursor()
        conn.autocommit = True
        cursor.execute(query)
        print(f'Query: {query}\n executed successfully')
        return cursor
    except Exception as e:
        print(f'Failed with error message: {e}')
        raise e
    finally:
        cursor.close()