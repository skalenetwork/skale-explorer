import logging
import os
import subprocess
from datetime import datetime
from time import sleep
import psycopg2

from admin import EXPLORER_SCRIPT_PATH, EXPLORERS_META_DATA_PATH, EXPLORER_VERSION, ABI_FILEPATH
from admin.configs.meta import is_current_version, is_schain_upgraded, verified_contracts, update_meta_data, \
    is_statistic_updated, update_statistic_ts, create_meta_file, get_explorers_meta, is_gas_prices_updated, \
    update_gas_prices_time, get_schain_meta
from admin.core.containers import (get_free_port, get_db_port, restart_nginx,
                                   is_explorer_running, remove_explorer)
from admin.core.endpoints import get_all_names, get_schain_endpoint, is_dkg_passed
from admin.migrations.gas_prices import update_schains_gas_prices
from admin.statistics.collector import update_schains_stats
from admin.statistics.database import create_tables
from admin.statistics.utils import download_gas_prices
from admin.utils.logger import init_logger
from admin.migrations.revert_reasons import upgrade, set_schain_upgraded
from admin.configs.nginx import regenerate_nginx_config
from admin.configs.schains import generate_config
from admin.core.verify import verify

logger = logging.getLogger(__name__)


def run_explorer(schain_name, endpoint, ws_endpoint):
    explorer_port = get_free_port()
    db_port = get_db_port(schain_name)
    config_host_path = generate_config(schain_name)
    env = {
        'SCHAIN_NAME': schain_name,
        'PORT': str(explorer_port),
        'DB_PORT': str(db_port),
        'ENDPOINT': endpoint,
        'WS_ENDPOINT': ws_endpoint,
        'CONFIG_PATH': config_host_path,
        'BLOCKSCOUT_VERSION': EXPLORER_VERSION
    }
    logger.info(f'Running explorer with {env}')
    logger.info('=' * 100)
    subprocess.run(['bash', EXPLORER_SCRIPT_PATH], env={**env, **os.environ})
    logger.info('=' * 100)
    update_meta_data(schain_name, explorer_port, db_port, endpoint, ws_endpoint, EXPLORER_VERSION)
    regenerate_nginx_config()
    restart_nginx()
    logger.info(f'sChain explorer is running on {schain_name}. subdomain')


def run_explorer_for_schain(schain_name):
    schain_meta = get_schain_meta(schain_name)
    if schain_meta and schain_meta.get('sync') is True:
        endpoint = schain_meta['endpoint']
        ws_endpoint = schain_meta['ws_endpoint']
    else:
        endpoint = get_schain_endpoint(schain_name)
        ws_endpoint = get_schain_endpoint(schain_name, ws=True)
    if endpoint and ws_endpoint:
        run_explorer(schain_name, endpoint, ws_endpoint)
    else:
        logger.warning(f"Couldn't create blockexplorer instance for {schain_name}")


def run_iteration():
    explorers = get_explorers_meta()
    schains = get_all_names()
    for schain_name in schains:
        if schain_name not in explorers and not is_dkg_passed(schain_name):
            continue
        if schain_name not in explorers:
            run_explorer_for_schain(schain_name)
            set_schain_upgraded(schain_name)
        if not is_explorer_running(schain_name) or not is_current_version(schain_name):
            if not is_explorer_running(schain_name):
                logger.warning(f'Blockscout is not working for {schain_name}. Recreating...')
            else:
                logger.warning(f'Blockscout version is outdated for {schain_name}. Recreating...')
            remove_explorer(schain_name)
            if not is_schain_upgraded(schain_name):
                upgrade(schain_name)
            run_explorer_for_schain(schain_name)
        if not verified_contracts(schain_name) and is_explorer_running(schain_name):
            verify(schain_name)
    if not is_gas_prices_updated():
        logger.info('Updating mainnet gas_prices for sChains...')
        current_date = str(datetime.today().date())
        download_gas_prices(end_date=current_date)
        status = update_schains_gas_prices(schains)
        if status:
            update_gas_prices_time(current_date)
    if not is_statistic_updated():
        try:
            logger.info('Collecting statistics...')
            ts = update_schains_stats(schains)
            update_statistic_ts(ts)
        except psycopg2.OperationalError as e:
            logger.warning(f'Collecting failed: {e}')


def main():
    assert os.path.isfile(ABI_FILEPATH), "ABI not found"
    if not os.path.isfile(EXPLORERS_META_DATA_PATH):
        create_meta_file()
    create_tables()
    while True:
        logger.info('Running new iteration...')
        run_iteration()
        sleep_time = 600
        logger.info(f'Sleeping {sleep_time}s')
        sleep(sleep_time)


if __name__ == '__main__':
    init_logger()
    main()
