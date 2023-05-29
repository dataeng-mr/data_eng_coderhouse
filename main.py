import os
from dotenv import load_dotenv
import pandas as pd
from util.framework import MarvelClient, redshift_connection, exec_sql_query

def main():
    try:
        load_dotenv()
        #Redshift details
        host = os.getenv('REDSHIFT_HOST')
        port = int(os.getenv('REDSHIFT_PORT'))
        database = os.getenv('REDSHIFT_DB')
        user = os.getenv('REDSHIFT_USER')
        password = os.getenv('REDSHIFT_PASS')
        schema = os.getenv('REDSHIFT_SCHEMA')
        conn, engine = redshift_connection(host, port, database, user, password)

        #Marvel details
        url = os.getenv('MARVEL_URL')
        public_key = os.getenv('MARVEL_PUBLIC_KEY')
        private_key = os.getenv('MARVEL_PRIVATE_KEY')
        marvel_client = MarvelClient(url, public_key, private_key)
        print('Marvel client succesfully created')

        #Create Redshift table for loading the data
        create_table_query = f'''CREATE TABLE IF NOT EXISTS {schema}.characters(
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description VARCHAR(MAX),
            modified TIMESTAMP NOT NULL
        )'''
        exec_sql_query(conn, create_table_query)
        truncate_table_query = f'''TRUNCATE TABLE {schema}.characters'''
        exec_sql_query(conn, truncate_table_query)

        #Creation of Pandas Dataframe and Redshift Table
        df = pd.DataFrame(marvel_client.get_characters_list())
        # Convert the timestamp column to datetime format
        df['modified'] = pd.to_datetime(df['modified'], utc=True)
        df['modified'] = df['modified'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df.to_sql('characters', engine, index=False, if_exists='append')
        print('Data succesfully loaded to Redshift')

    except Exception as e:
        print(f'Failed with error message: {e}')
    finally:
        conn.close()

if __name__ == '__main__':
    main()