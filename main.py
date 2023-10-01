import aiohttp
import asyncio
import pandas as pd
from sqlalchemy import create_engine

# Define the PostgreSQL credentials
USERNAME = 'postgres'
PASSWORD = 'aadsh1892'
DB_NAME = 'countries'
HOST = 'localhost'

# Connect to PostgreSQL server
alchemyEngine = create_engine(f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}/{DB_NAME}', pool_recycle=3600)
dbConnection = alchemyEngine.connect()

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

async def fetch_country_data(session, url):
    async with session.get(url, headers = headers) as response:
        return await response.json()

async def get_northern_european_countries_data():
    url = "https://restcountries.com/v3.1/all"
    async with aiohttp.ClientSession() as session:
        countries_data = await fetch_country_data(session, url)
        
        # Filter for Northern European countries and specific fields
        filtered_data = []
        for country in countries_data:
            region = country.get('region', '')
            if region.lower() == 'europe' and country.get('subregion', '').lower() == 'northern europe':
                filtered_data.append({
                    'nation_official_name': country.get('name', {}).get('official', ''),
                    'currency_name': list(country.get('currencies', {}).keys())[0] if country.get('currencies', {}) else '',
                    'population': country.get('population', 0)
                })
                
        # Load the filtered data into a single index Pandas DataFrame
        df = pd.DataFrame(filtered_data)
        return df

# Run the async function to get the data
loop = asyncio.get_event_loop()
df = loop.run_until_complete(get_northern_european_countries_data())
print(df)

try:

    tableName = "northern_european_countries"
    
    # Replace mode: If table exists, drop it and create a new one
    df.to_sql(tableName, dbConnection, if_exists='replace', index=False)
    
finally:
    dbConnection.close()