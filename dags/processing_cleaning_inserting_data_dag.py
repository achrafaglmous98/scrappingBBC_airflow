from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
from datetime import datetime, timedelta
import pymongo
import shutil
import os

def clean_and_push_data():

    print("Task Starts...")

    # Find the most recent CSV file in the directory with the given prefix
    files = [f for f in os.listdir('./scrapped_data') if f.startswith('bbc_articles_') and f.endswith('.csv')]

    if files:
        filename = min(files, key=lambda x: os.path.basename(x))
        print(f"Found oldest file: {filename}")
    else:
        print("No matching files found")

    filepath = f'./scrapped_data/{filename}'
    
    # Read data from CSV file
    df = pd.read_csv(filepath)
    print(df.head())
    
    # Clean data
    df.drop_duplicates(inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df['Category'] = df['Category'].astype(str)
    df['Subcategory'] = df['Subcategory'].astype(str)
    df['Subcategory'] = df['Subcategory'].apply(lambda x: x.replace("selected", "") if "selected" in x else x)
    df['Topic'] = df['Topic'].astype(str)
    df['Authors'] = df['Authors'].apply(get_list_of_authors)
    df.fillna(value="N/A", inplace=True)

    # Create connection to MongoDB using MongoHook
    mongo_hook = MongoHook(conn_id="mongo_default")
    db_name = "deep_search_lab_app"
    coll_name = "bbc_articles"
    db = mongo_hook.get_conn()[db_name]
    
    # Create collection if it doesn't exist
    if coll_name not in db.list_collection_names():
        db.create_collection(coll_name)

    collection = db[coll_name]

    # Loop through each record in the DataFrame and check if it already exists in the collection
    for record in df.to_dict('records'):
        query_fields = {
            'Title': record['Title'],
            'Date': record['Date'],
            'Category': record['Category'],
            'Subcategory': record['Subcategory'],
            'Topic': record['Topic'],
            'Images': record['Images']
        }
        existing_record = collection.find_one(query_fields)
        if existing_record is None:
            # Insert the record into the collection if it does not already exist
            collection.insert_one(record)
        else:
            # Update the existing record if it does exist
            record_id = existing_record['_id']
            collection.replace_one({'_id': record_id}, record)
    
    # Move file to another directory
    dest_dir = './processed_data/'
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    shutil.move(filepath, dest_dir)

def remove_after_cap(string):

    #Remove all characters that come after a directly attached capital letter in a string
    new_string = ''
    for i, char in enumerate(string):
        if i == 0:
            new_string += char
        elif char.isupper() and string[i-1].islower():
            if string[i-2 : i] == 'Mc':
                new_string += char
            else:    
                break
        else:
            new_string += char
    return new_string

def get_list_of_authors(authors):
    
    if not isinstance(authors, str):
        return ["N/A"]
    
    # Remove any characters that come after a directly attached capital letter
    authors = remove_after_cap(authors)

    # Split the string by "and"
    parts = authors.replace(" & ", " and ").split(" and ")

    # Extract the author names from each part
    authors = []
    for part in parts:
        if "By " in part:
            first_author_name = part.split("By ")[1].split()
            author = " ".join(first_author_name[:2])
            authors.append(author)
        else:
            name_words = part.strip().split()
            author = " ".join(name_words[:2])
            authors.append(author)

    # Return the list of author names
    return authors

default_args = {
    'owner': 'Achraf',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 14),
    'retries': 1,
}

dag = DAG('clean_and_push_data', 
          default_args=default_args, 
          schedule_interval='0 23 * * *')

file_sensor_task = FileSensor(
    task_id ='file_sensor_task',
    filepath ='./scrapped_data/bbc_articles_*.csv',
    poke_interval = 300,
    dag = dag,
)

clean_and_push_task = PythonOperator(
    task_id='clean_and_push_task',
    python_callable=clean_and_push_data,
    dag=dag,
)

file_sensor_task >> clean_and_push_task