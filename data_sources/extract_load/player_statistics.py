import requests
import json
import pandas as pd
from google.cloud import storage, bigquery
import os
from dotenv import load_dotenv
import aiohttp
import asyncio

load_dotenv()
bucket = os.environ.get('BUCKET')
project = os.environ.get('PROJECT')
dataset = os.environ.get('DATASET')
table = os.environ.get('TABLE')
api_key = os.environ.get('API_KEY')

file_path = './data.json'
with open(file_path, 'r') as file:
    data = json.load(file)

sportsradar_base_url = "https://api.sportradar.com/nba/trial/v8/en/seasons/2022/REG/teams/583eca2f-fb46-11e1-82cb-f4ce4684ea4c/statistics.json"

async def fetch_data(session, url, params):
    async with session.get(url, params=params) as response:
        return await response.json()  

async def get_player_stats():
    async with aiohttp.ClientSession() as session:
        tasks = []
        team_ids = [
            '583eccfa-fb46-11e1-82cb-f4ce4684ea4c',
            '583ec87d-fb46-11e1-82cb-f4ce4684ea4c',
            '583ec70e-fb46-11e1-82cb-f4ce4684ea4c',
            '583ec9d6-fb46-11e1-82cb-f4ce4684ea4c'
            # '583ecda6-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecefd-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ec773-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ec7cd-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ec5fd-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ec928-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecea6-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ed157-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecb8f-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ec97e-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ec8d4-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecfff-fb46-11e1-82cb-f4ce4684ea4c',
            # '583eca2f-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ed102-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ece50-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ed056-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecdfb-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ed0ac-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecfa8-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecae2-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ec825-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecc9a-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecf50-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecb3a-fb46-11e1-82cb-f4ce4684ea4c',
            # '583eca88-fb46-11e1-82cb-f4ce4684ea4c',
            # '583ecd4f-fb46-11e1-82cb-f4ce4684ea4c'
        ]
        for team in team_ids: 
            url = "https://api.sportradar.com/nba/trial/v8/en/seasons/2023/REG/teams/%s/statistics.json" % team
            params = {'api_key': api_key, 'Accept': 'application/json'}  
            task = asyncio.create_task(fetch_data(session, url, params))
            tasks.append(task)

        return await asyncio.gather(*tasks)

responses = asyncio.run(get_player_stats())
data_rows = []

for response in responses:
    team_id = data.get('id', '')
    players = data.get('players', [])

    for player in players:
        totals = player.get("total", {})
        player_data = {
            "id": player["id"],
            'team_id' : team_id,
            "full_name": player["full_name"],
            "position": player["primary_position"],
            "jersey_number": player.get("jersey_number", None),
             **totals
            }
        data_rows.append(player_data)

df = pd.DataFrame(data_rows)

csv_file_path = 'player_statistics.csv'
df.to_csv(csv_file_path, index=False)

# Google Cloud Storage parameters
bucket_name = bucket
destination_blob_name = 'player_statistics.csv'
source_file_name = 'player_statistics.csv'

# # Upload file to Google Cloud Storage
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(source_file_name)