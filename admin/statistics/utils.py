import cfscrape
from admin import GAS_PRICES_FILEPATH


def download_gas_prices():
    scraper = cfscrape.create_scraper()
    url = 'https://etherscan.io/chart/gasprice?output=csv'
    response = scraper.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    with open(GAS_PRICES_FILEPATH, 'w') as f:
        f.write(response.text)
