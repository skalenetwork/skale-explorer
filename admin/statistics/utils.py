import json
import logging

import requests

from admin import GAS_PRICES_FILEPATH, ETH_API_KEY
from admin.utils.helper import write_json

logger = logging.getLogger(__name__)


def download_gas_prices(end_date, start_date=None):
    start_date = start_date if start_date else '2021-01-01'
    url = f'https://api.etherscan.io/api?module=stats&action=dailyavggasprice&' \
          f'startdate={start_date}&' \
          f'enddate={end_date}&' \
          f'sort=asc&' \
          f'apikey={ETH_API_KEY}'
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        data = json.loads(response.text)['result']
        write_json(GAS_PRICES_FILEPATH, data)
    except requests.RequestException as e:
        logger.warning(f'Could not download gas_prices: {e}')

