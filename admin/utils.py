import logging

from admin import EXPLORERS_META_DATA_PATH, EXPLORER_VERSION, ZERO_ADDRESS
from admin.endpoints import read_json, write_json

logger = logging.getLogger(__name__)

def verified_contracts(schain_name):
    explorers = read_json(EXPLORERS_META_DATA_PATH)
    return explorers[schain_name].get('contracts_verified') is True


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


def is_current_version(schain_name):
    explorers = read_json(EXPLORERS_META_DATA_PATH)
    return explorers[schain_name].get('version') == EXPLORER_VERSION


def get_schain_originator(schain: dict):
    if schain['originator'] == ZERO_ADDRESS:
        return schain['mainnetOwner']
    return schain['originator']
