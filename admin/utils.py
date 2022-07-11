import logging
from os.path import join

from admin import ZERO_ADDRESS, SCHAIN_CONFIG_DIR_PATH
from admin.endpoints import read_json, write_json

logger = logging.getLogger(__name__)


def get_schain_originator(schain: dict):
    if schain['originator'] == ZERO_ADDRESS:
        return schain['mainnetOwner']
    return schain['originator']


def set_contract_verified(schain_name, address):
    path = join(SCHAIN_CONFIG_DIR_PATH, f'{schain_name}.json')
    config = read_json(path)
    config['verification_status'][address] = True
    write_json(path, config)

