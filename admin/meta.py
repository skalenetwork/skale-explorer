import logging
from os.path import isfile

from admin import EXPLORERS_META_DATA_PATH, EXPLORER_VERSION
from admin.endpoints import read_json, write_json

logger = logging.getLogger(__name__)


def is_schain_upgraded(schain_name):
    explorers = read_json(EXPLORERS_META_DATA_PATH)
    schain_meta = explorers.get(schain_name)
    if not schain_meta or schain_meta.get('updated'):
        return True


def is_current_version(schain_name):
    explorers = read_json(EXPLORERS_META_DATA_PATH)
    return explorers[schain_name].get('version') == EXPLORER_VERSION


def verified_contracts(schain_name):
    explorers = read_json(EXPLORERS_META_DATA_PATH)
    return explorers[schain_name].get('contracts_verified') is True


def set_schain_upgraded(schain_name):
    meta = read_json(EXPLORERS_META_DATA_PATH)
    schain_meta = meta[schain_name]
    schain_meta['updated'] = True
    meta.update({
        schain_name: schain_meta
    })
    write_json(EXPLORERS_META_DATA_PATH, meta)


def update_meta_data(schain_name, port, db_port, endpoint, ws_endpoint, version):
    logger.info(f'Updating meta data for {schain_name}')
    explorers = read_json(EXPLORERS_META_DATA_PATH) if isfile(EXPLORERS_META_DATA_PATH) else {}
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
    write_json(EXPLORERS_META_DATA_PATH, explorers)


def get_schain_endpoint(schain_name):
    data = read_json(EXPLORERS_META_DATA_PATH)
    schain_endpoint = data[schain_name]['endpoint']
    return schain_endpoint


def get_explorer_endpoint(schain_name):
    explorers = read_json(EXPLORERS_META_DATA_PATH)
    return f'http://127.0.0.1:{explorers[schain_name]["port"]}'
