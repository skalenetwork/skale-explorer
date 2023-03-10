import json
import logging
from admin import ZERO_ADDRESS

logger = logging.getLogger(__name__)


def get_schain_originator(schain: dict):
    if schain['originator'] == ZERO_ADDRESS:
        return schain['mainnetOwner']
    return schain['originator']


def read_json(path, mode='r'):
    with open(path, mode=mode, encoding='utf-8') as data_file:
        return json.load(data_file)


def write_json(path, content):
    with open(path, 'w') as outfile:
        json.dump(content, outfile, indent=4)
