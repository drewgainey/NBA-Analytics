import pandas as pd
import requests
from google.cloud import storage, bigquery
import os
from dotenv import load_dotenv

load_dotenv()
bucket = os.environ.get('BUCKET')
project = os.environ.get('PROJECT')
dataset = os.environ.get('DATASET')
table = os.environ.get('TABLE')
api_key = os.environ.get('API_KEY')


url = "http://api.sportradar.us/nba/trial/v8/en/seasons/2023/REG/rankings.json"
params = {
    'api_key': api_key
}

response = requests.get(url, params=params)
json_data = response.json()

def flatten_teams_data(json_data):
    teams_data = []
    for conference in json_data['conferences']:
        conference_id = conference['id']
        conference_name = conference['name']
        for division in conference['divisions']:
            division_id = division['id']
            division_name = division['name']
            for team in division['teams']:
                team_data = {
                    'conference_id': conference_id,
                    'conference_name': conference_name,
                    'division_id': division_id,
                    'division_name': division_name,
                    'team_id': team['id'],
                    'team_name': team['name'],
                    'team_market': team['market'],
                    'rank_conference': team['rank']['conference'],
                    'rank_division': team['rank']['division']
                }
                teams_data.append(team_data)
    return teams_data

flattened_data = flatten_teams_data(json_data)

df = pd.DataFrame(flattened_data)

csv_file_path = './team_rankings.csv'
df.to_csv(csv_file_path, index=False)

# Google Cloud Storage parameters
bucket_name = bucket
destination_blob_name = 'team_rankings.csv'
source_file_name = 'team_rankings.csv'

# # Upload file to Google Cloud Storage
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)
