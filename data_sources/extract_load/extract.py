import psycopg2
from google.cloud import storage, bigquery
import os
from dotenv import load_dotenv

load_dotenv()
bucket = os.environ.get('BUCKET')
project = os.environ.get('PROJECT')
dataset = os.environ.get('DATASET')
table = os.environ.get('TABLE')

# PostgreSQL connection parameters
db_params = {
    'host': 'source_db',  # Name of your PostgreSQL Docker container
    'database': 'source_db',
    'user': 'postgres',
    'password': 'secret'  # Use the password you set for PostgreSQL
}

# Connect to PostgreSQL DB
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# SQL query to extract data
sql_query = 'SELECT * FROM nba_rankings'

# Execute query
cursor.execute(sql_query)

# Fetch the data
data = cursor.fetchall()

# Format data as needed and write to a CSV file
with open('data.csv', 'w') as file:
    for row in data:
        file.write(','.join(map(str, row)) + '\n')

cursor.close()
conn.close()

# Google Cloud Storage parameters
bucket_name = bucket
destination_blob_name = 'nba_rankings.csv'
source_file_name = 'data.csv'

# Upload file to Google Cloud Storage
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)

# Google BigQuery parameters
project_id = 'your_project_id'
dataset_id = 'your_dataset_id'
table_id = 'your_table_id'

# Load data into BigQuery
client = bigquery.Client(project=project_id)
table_ref = client.dataset(dataset_id).table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.autodetect = True

uri = f'gs://{bucket_name}/{destination_blob_name}'
load_job = client.load_table_from_uri(
    uri, table_ref, job_config=job_config
)

# Waits for the job to complete
load_job.result()
