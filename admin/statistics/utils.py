import requests
from admin import GAS_PRICES_FILEPATH


def download_gas_prices():
    url = 'https://etherscan.io/chart/gasprice?output=csv'
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    with open(GAS_PRICES_FILEPATH, 'w') as f:
        f.write(response.text)
