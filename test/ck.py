import requests
import json
import pandas as pd
import codecs
from tabulate import tabulate  # Import the tabulate module

api_url = "https://api.adswizz.com/domain/v7/reports/query"
api_key = "3Ad8CexcwpQYcncwfUFu68ZKYrHuGvvJU7jjJwy2"
agency_id = "10"
environment = "AUDIOMAX"

# Set up the headers with the x-api-key
headers = {
    'x-api-key': api_key,
    'agency': agency_id,
    'environment': environment
}

json_data = {
    "filter": {
        "type": "AND",
        "fields": [{
            "id": "geoCity",
            "type": "IN",
            "values": ["new york", "las vegas"]
        }, {
            "id": "contractLineName",
            "type": "IN",
            "values": ["MyAwesomeCampaign(239)", "MyProfitableCampaign(240)"]
        }]},
    "splitters": [{
        "id": "contractLineName",
        "limit": 20
    }],
    "metrics": ["objectiveCountableSum",  "supplyRevenueInUSD", "supplyGrossClearingPriceSum"],
    "interval": {
        "from": "2023-11-01T00:00:00Z",
        "to": "2023-11-30T23:59:59Z"
    },
    "sort": {
        "id": "objectiveCountableSum",
        "dir": "DESC"
    }
}


# Convert the dictionary to JSON format
json_data_str = json.dumps(json_data)

# Make the POST request instead of PUT
r = requests.post(api_url, headers=headers, data=json_data_str)

# Check the response status
if r.status_code == 200:
    try:
        # Attempt to extract the relevant data from the JSON response
        response_data = json.loads(codecs.decode(bytes(r.text, 'utf-8'), 'utf-8-sig'))
        df = pd.DataFrame(json.loads(codecs.decode(bytes(r.text, 'utf-8'), 'utf-8-sig'))['data'])

        # Display the DataFrame as a table using tabulate
        table = tabulate(df, headers='keys', tablefmt='pretty')
        print(r.text)
    except KeyError as e:
        print(f"KeyError: {e}")
        print("Response structure may have changed. Inspect the response:")
        print(response_data)
else:
    print(f"Error: {r.status_code}")
    #print(r.text)


