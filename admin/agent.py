import logging
import os
from datetime import datetime
from functools import wraps
from threading import Thread
from time import sleep
import psycopg2

from admin import EXPLORERS_META_DATA_PATH, ABI_FILEPATH
from admin.configs.meta import (is_statistic_updated, update_statistic_ts, create_meta_file,
                                is_gas_prices_updated, update_gas_prices_time)
from admin.core.endpoints import get_all_names
from admin.core.explorers import check_explorer_for_schain
from admin.migrations.gas_prices import update_schains_gas_prices
from admin.statistics.collector import update_schains_stats
from admin.statistics.database import create_tables
from admin.statistics.utils import download_gas_prices
from admin.utils.logger import init_logger

logger = logging.getLogger(__name__)


def daemon(delay=60):
    def actual_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f'Initiating {func.__name__}')
            while True:
                try:
                    func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f'{func.__name__} failed with: {e}')
                    logger.warning(f'Restarting {func.__name__}...')
                sleep(delay)
        return wrapper
    return actual_decorator


@daemon()
def check_explorer_status():
    schains = get_all_names()
    for schain_name in schains:
        check_explorer_for_schain(schain_name)


@daemon()
def collect_statistics():
    if not is_statistic_updated():
        schains = get_all_names()
        try:
            logger.info('Collecting statistics...')
            ts = update_schains_stats(schains)
            update_statistic_ts(ts)
        except psycopg2.OperationalError as e:
            logger.warning(f'Collecting failed: {e}')


@daemon()
def update_gas_prices():
    if not is_gas_prices_updated():
        schains = get_all_names()
        logger.info('Updating mainnet gas_prices for sChains...')
        current_date = str(datetime.today().date())
        download_gas_prices(end_date=current_date)
        status = update_schains_gas_prices(schains)
        if status:
            update_gas_prices_time(current_date)


def main():
    assert os.path.isfile(ABI_FILEPATH), "ABI not found"
    if not os.path.isfile(EXPLORERS_META_DATA_PATH):
        create_meta_file()
    create_tables()

    Thread(target=check_explorer_status, daemon=True, name='explorers-checker').start()
    Thread(target=update_gas_prices, daemon=True, name='gas-price-checker').start()
    Thread(target=collect_statistics, daemon=True, name='stats-collector').start()
    while True:
        sleep(1)


if __name__ == '__main__':
    init_logger()
    main()
