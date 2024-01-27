import requests
import pandas as pd
import xml.etree.ElementTree as ET
import psycopg2

# Step 1: Make an API request
try:
    url = "http://api.sportradar.us/nba/trial/v8/en/seasons/2023/REG/rankings.xml?api_key=6ev5erarhn5xa9wzez3h88gb"
    response = requests.get(url)
    xml_data = response.content
except:
    print("Error on API call")

# Parse the XML
tree = ET.ElementTree(ET.fromstring(xml_data))
root = tree.getroot()

# Namespace to correctly find elements
namespace = {'ns': 'http://feed.elasticstats.com/schema/basketball/nba/rankings-v3.0.xsd'}

# List to store each team's data
teams_data = []

# Iterate through each element to extract team data
for team in root.findall('.//ns:team', namespace):
    team_data = {
        'team_id': team.attrib.get('id'),
        'name': team.attrib.get('name'),
        'market': team.attrib.get('market'),
        'sr_id': team.attrib.get('sr_id'),
        'reference': team.attrib.get('reference'),
        'conference_rank': team.find('ns:rank', namespace).attrib.get('conference'),
        'division_rank': team.find('ns:rank', namespace).attrib.get('division')
    }
    teams_data.append(team_data)

# PostgreSQL connection parameters
db_params = {
    'host': 'source_db',  # Name of your PostgreSQL Docker container
    'database': 'source_db',
    'user': 'postgres',
    'password': 'secret'  # Use the password you set for PostgreSQL
}

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

for team in teams_data:
    cursor.execute(
        'INSERT INTO nba_rankings (team_id, name, market, sr_id, reference, conference_rank, division_rank) VALUES (%s, %s, %s, %s, %s, %s, %s)',
        (team['team_id'], team['name'], team['market'], team['sr_id'], team['reference'], team['conference_rank'], team['division_rank'])   
    )

conn.commit()
cursor.close()
conn.close()
