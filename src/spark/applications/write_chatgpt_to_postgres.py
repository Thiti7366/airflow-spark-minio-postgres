import psycopg2
import traceback
import logging
import sys
import os
import json
import urllib.request

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

# PostgreSQL connection parameters from arguments
postgres_host = sys.argv[1]
postgres_database = sys.argv[2]
postgres_user = sys.argv[3]
postgres_password = sys.argv[4]
postgres_port = sys.argv[5]

url = "https://datasets-server.huggingface.co/rows?dataset=fka%2Fawesome-chatgpt-prompts&config=default&split=train&offset=0&length=100"
dest_folder = '/tmp'  # กำหนด folder สำหรับเก็บไฟล์
destination_path = os.path.join(dest_folder, 'awesome_chatgpt_prompts.json')

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

def download_file_from_url(url: str, dest_folder: str):
    """
    Download a file from a specific URL and download to the local directory
    """
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    try:
        urllib.request.urlretrieve(url, destination_path)
        logging.info('JSON file downloaded successfully to the working directory')
    except Exception as e:
        logging.error(f'Error while downloading the JSON file due to: {e}')
        traceback.print_exc()

def create_postgres_table():
    """
    Create the Postgres table with a desired schema
    """
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS awesome_chatgpt_prompts (
            RowNumber SERIAL PRIMARY KEY, prompt TEXT, category VARCHAR(255));""")
        
        logging.info('New table awesome_chatgpt_prompts created successfully in Postgres server')
    except Exception as e:
        logging.warning('Check if the table awesome_chatgpt_prompts exists')
        traceback.print_exc()

def write_to_postgres():
    """
    Create the dataframe and write to Postgres table if it doesn't already exist
    """
    with open(destination_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    inserted_row_count = 0
    for row in data['rows']:
        prompt = row['row']['prompt']
        category = row['row'].get('category', None)  # Modify based on actual schema
        
        count_query = f"""SELECT COUNT(*) FROM awesome_chatgpt_prompts WHERE prompt = %s"""
        cur.execute(count_query, (prompt,))
        result = cur.fetchone()
        
        if result[0] == 0:
            cur.execute("""INSERT INTO awesome_chatgpt_prompts (prompt, category) 
                VALUES (%s, %s);""", (prompt, category))
            inserted_row_count += 1
            
    logging.info(f'{inserted_row_count} rows from JSON file inserted into awesome_chatgpt_prompts table successfully')

if __name__ == '__main__':
    download_file_from_url(url, dest_folder)
    create_postgres_table()
    write_to_postgres()
    conn.commit()
    cur.close()
    conn.close()
