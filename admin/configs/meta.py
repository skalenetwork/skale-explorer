
import logging
from datetime import datetime
from time import time

from admin import EXPLORERS_META_DATA_PATH, EXPLORER_VERSION, STATS_TIME_DELTA
from admin.utils.helper import read_json, write_json

logger = logging.getLogger(__name__)


def create_meta_file():
    empty_data = {
        'explorers': {}
    }
    write_json(EXPLORERS_META_DATA_PATH, empty_data)


def is_schain_upgraded(schain_name):
    schain_meta = get_schain_meta(schain_name)
    if not schain_meta or schain_meta.get('updated'):
        return True


def is_current_version(schain_name):
    return get_schain_meta(schain_name).get('version') == EXPLORER_VERSION


def verified_contracts(schain_name):
    return get_schain_meta(schain_name).get('contracts_verified') is True


def set_schain_upgraded(schain_name):
    meta = read_json(EXPLORERS_META_DATA_PATH)
    schain_meta = meta['explorers'][schain_name]
    schain_meta['updated'] = True
    write_json(EXPLORERS_META_DATA_PATH, meta)


def update_meta_data(schain_name, port, db_port, endpoint, ws_endpoint, version):
    logger.info(f'Updating meta data for {schain_name}')
    meta_data = read_json(EXPLORERS_META_DATA_PATH)
    explorers = meta_data['explorers']
    schain_meta = explorers.get(schain_name, {})
    schain_meta.update({
        'port': port,
        'db_port': db_port,
        'endpoint': endpoint,
        'ws_endpoint': ws_endpoint,
        'version': version,
    })
    explorers.update({
        schain_name: schain_meta
    })
    write_json(EXPLORERS_META_DATA_PATH, meta_data)


def get_schain_endpoint(schain_name):
    return get_schain_meta(schain_name)['endpoint']


def get_explorer_endpoint(schain_name):
    explorer_port = get_schain_meta(schain_name)['port']
    return f'http://127.0.0.1:{explorer_port}'


def get_schain_meta(schain_name):
    data = get_explorers_meta()
    return data.get(schain_name)


def set_chain_verified(schain_name):
    data = read_json(EXPLORERS_META_DATA_PATH)
    data['explorers'][schain_name]['contracts_verified'] = True
    write_json(EXPLORERS_META_DATA_PATH, data)


def get_explorers_meta():
    return read_json(EXPLORERS_META_DATA_PATH)['explorers']


def is_statistic_updated():
    data = read_json(EXPLORERS_META_DATA_PATH)
    last_updated = data.get('stats_last_updated')
    if not last_updated:
        return False
    if time() - last_updated < STATS_TIME_DELTA:
        return True
    return False


def update_statistic_ts(ts):
    logger.info(f'Update last statistic ts: {ts}')
    data = read_json(EXPLORERS_META_DATA_PATH)
    data['stats_last_updated'] = ts
    write_json(EXPLORERS_META_DATA_PATH, data)


def is_gas_prices_updated():
    data = read_json(EXPLORERS_META_DATA_PATH)
    last_updated = data.get('gas_price_last_updated')
    if not last_updated:
        return False
    current_date = datetime.today().date()
    last_updated_date = datetime.strptime(last_updated, '%Y-%m-%d').date()
    if last_updated_date < current_date:
        return False
    return True


def update_gas_prices_time(date):
    logger.info(f'Update last gas_price date: {date}')
    data = read_json(EXPLORERS_META_DATA_PATH)
    data['gas_price_last_updated'] = date
    write_json(EXPLORERS_META_DATA_PATH, data)


def get_gas_prices_update_time():
    data = read_json(EXPLORERS_META_DATA_PATH)
    last_updated = data.get('gas_price_last_updated')
    return last_updated
