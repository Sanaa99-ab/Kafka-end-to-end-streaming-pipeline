import requests
import json

url = 'https://api.coingecko.com/api/v3/coins/list'

response = requests.get(url)
if response.status_code == 200:
    # Print the fetched data
    data = response.json()
    print(json.dumps(data[:], indent=2))  # Display data for the first 10 cryptocurrencies in a readable format
else:
    print('Failed to fetch data:', response.status_code)
