import psycopg2
import traceback
import logging
import sys
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

# PostgreSQL connection parameters from arguments
postgres_host = sys.argv[1]
postgres_database = sys.argv[2]
postgres_user = sys.argv[3]
postgres_password = sys.argv[4]
postgres_port = sys.argv[5]

try:
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )
    cur = conn.cursor()
    logging.info('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    logging.error("Couldn't create the Postgres connection")

def load_chatgpt_prompts():
    """
    Load dataset 'awesome-chatgpt-prompts' using requests
    """
    url = "https://datasets-server.huggingface.co/rows?dataset=fka%2Fawesome-chatgpt-prompts&config=default&split=train&offset=0&length=100"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()
        prompts = [(row['row']['prompt'], row['row'].get('category', None)) for row in data['rows']]
        df = pd.DataFrame(prompts, columns=['prompt', 'category'])
        logging.info('Dataset loaded successfully')
        return df
    except Exception as e:
        logging.error(f'Error loading dataset: {e}')
        traceback.print_exc()
        return None

def create_postgres_table():
    """
    Create the Postgres table with the desired schema for storing ChatGPT prompts.
    """
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS chatgpt_prompts (
            RowNumber SERIAL PRIMARY KEY, 
            category VARCHAR(255), 
            prompt TEXT
        );""")
        logging.info('New table chatgpt_prompts created successfully on the Postgres server')
    except Exception as e:
        logging.warning('Error creating table, check if the table already exists')
        traceback.print_exc()

def write_to_postgres(dataset):
    """
    Write the dataset to the Postgres table.
    """
    inserted_row_count = 0

    for index, row in dataset.iterrows():
        # Check if the row already exists
        count_query = f"""SELECT COUNT(*) FROM chatgpt_prompts WHERE prompt = %s"""
        cur.execute(count_query, (row['prompt'],))
        result = cur.fetchone()

        if result[0] == 0:  # If row does not exist, insert it
            cur.execute("""INSERT INTO chatgpt_prompts (category, prompt) 
                           VALUES (%s, %s);""", (row['category'], row['prompt']))
            inserted_row_count += 1
    
    logging.info(f'{inserted_row_count} rows from dataset inserted into chatgpt_prompts table successfully')

def read_from_postgres():
    """
    Read data from the Postgres table and print it
    """
    try:
        cur.execute("SELECT * FROM chatgpt_prompts;")
        rows = cur.fetchall()
        logging.info(f'Row data count: {len(rows)}')

        # Print only the first 10 rows for brevity
        for i, row in enumerate(rows[:10]):
            logging.info(f'Row {i+1}: {row}')
    
    except Exception as e:
        logging.error(f'Error while reading from Postgres table due to: {e}')
        traceback.print_exc()

if __name__ == '__main__':
    dataset = load_chatgpt_prompts()
    if dataset is not None:
        create_postgres_table()
        write_to_postgres(dataset)
        read_from_postgres()
        conn.commit()
    else:
        logging.error("Cannot load dataset, exiting...")
    
    cur.close()
    conn.close()
