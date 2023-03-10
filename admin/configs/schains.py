import logging
import os
from os.path import join

from etherbase_predeployed.etherbase_upgradeable_generator import EtherbaseUpgradeableGenerator
from marionette_predeployed.marionette_generator import MarionetteGenerator
from web3 import Web3, HTTPProvider

from admin import (
    SCHAIN_CONFIG_DIR_PATH, PROXY_ADMIN_PREDEPLOYED_ADDRESS,
    ETHERBASE_ALLOC, SCHAIN_OWNER_ALLOC, NODE_OWNER_ALLOC,
    HOST_SCHAIN_CONFIG_DIR_PATH
)
from admin.core.endpoints import get_schain_info, get_schain_endpoint

from etherbase_predeployed import (
    UpgradeableEtherbaseUpgradeableGenerator, ETHERBASE_ADDRESS, ETHERBASE_IMPLEMENTATION_ADDRESS
)
from marionette_predeployed import (
    UpgradeableMarionetteGenerator, MARIONETTE_ADDRESS, MARIONETTE_IMPLEMENTATION_ADDRESS
)
from filestorage_predeployed import (
    UpgradeableFileStorageGenerator, FILESTORAGE_ADDRESS, FILESTORAGE_IMPLEMENTATION_ADDRESS,
    FileStorageGenerator
)
from config_controller_predeployed import (
    UpgradeableConfigControllerGenerator,
    CONFIG_CONTROLLER_ADDRESS,
    CONFIG_CONTROLLER_IMPLEMENTATION_ADDRESS, ConfigControllerGenerator
)
from multisigwallet_predeployed import MultiSigWalletGenerator, MULTISIGWALLET_ADDRESS
from context_predeployed import ContextGenerator, CONTEXT_ADDRESS
from predeployed_generator.openzeppelin.proxy_admin_generator import ProxyAdminGenerator
from ima_predeployed.generator import generate_meta

from admin.utils.helper import write_json, get_schain_originator, read_json

logger = logging.getLogger(__name__)


def get_schain_config(schain_name):
    return read_json(join(SCHAIN_CONFIG_DIR_PATH, f'{schain_name}.json'))


def write_schain_config(schain_name, config):
    write_json(join(SCHAIN_CONFIG_DIR_PATH, f'{schain_name}.json'), config)


def generate_config(schain_name):
    config_path = os.path.join(SCHAIN_CONFIG_DIR_PATH, f'{schain_name}.json')
    if not os.path.exists(config_path):
        logger.info(f'Generating config for {schain_name}')
        verification_data = generate_verify_data()
        addresses = verification_data.keys()
        verification_status_data = {address: False for address in addresses}
        config = {
            'alloc': {
                **fetch_predeployed_info(schain_name, addresses),
                **generate_owner_accounts(schain_name)
            },
            'verify': verification_data,
            'verification_status': verification_status_data
        }
        write_json(config_path, config)
    host_config_path = os.path.join(HOST_SCHAIN_CONFIG_DIR_PATH, f'{schain_name}.json')
    return host_config_path


def generate_owner_accounts(schain_name):
    schain_info = get_schain_info(schain_name)
    accounts = {}
    if schain_info['generation'] == 0:
        add_to_accounts(accounts, schain_info['mainnetOwner'], SCHAIN_OWNER_ALLOC)
    if schain_info['generation'] == 1:
        add_to_accounts(accounts, get_schain_originator(schain_info), SCHAIN_OWNER_ALLOC)
    for wallet in schain_info['nodes']:
        add_to_accounts(accounts, wallet, NODE_OWNER_ALLOC)
    return accounts


def add_to_accounts(accounts, address, balance=0, nonce=0, code=""):
    fixed_address = Web3.toChecksumAddress(address)
    account = {
        'balance': str(balance),
    }
    if code:
        account.update({
            'code': code,
            'nonce': hex(nonce),
            'storage': {}
        })
    accounts[fixed_address] = account


def generate_verify_data():
    raw_verification_dict = {
        PROXY_ADMIN_PREDEPLOYED_ADDRESS: ProxyAdminGenerator().get_meta(),
        CONTEXT_ADDRESS: ContextGenerator().get_meta(),
        CONFIG_CONTROLLER_ADDRESS: UpgradeableConfigControllerGenerator().get_meta(),
        CONFIG_CONTROLLER_IMPLEMENTATION_ADDRESS: ConfigControllerGenerator().get_meta(),
        MARIONETTE_ADDRESS: UpgradeableMarionetteGenerator().get_meta(),
        MARIONETTE_IMPLEMENTATION_ADDRESS: MarionetteGenerator().get_meta(),
        ETHERBASE_ADDRESS: UpgradeableEtherbaseUpgradeableGenerator().get_meta(),
        ETHERBASE_IMPLEMENTATION_ADDRESS: EtherbaseUpgradeableGenerator().get_meta(),
        MULTISIGWALLET_ADDRESS: MultiSigWalletGenerator().get_meta(),
        FILESTORAGE_ADDRESS: UpgradeableFileStorageGenerator().get_meta(),
        FILESTORAGE_IMPLEMENTATION_ADDRESS: FileStorageGenerator().get_meta(),
        **generate_meta()
    }
    return {
        Web3.toChecksumAddress(k): raw_verification_dict[k]
        for k in raw_verification_dict
    }


def fetch_predeployed_info(schain_name, contract_addresses):
    predeployed_contracts = {}
    schain_endpoint = get_schain_endpoint(schain_name)
    provider = HTTPProvider(schain_endpoint)
    web3 = Web3(provider)
    for address in contract_addresses:
        code = web3.eth.get_code(address).hex()
        if address == ETHERBASE_ADDRESS:
            add_to_accounts(predeployed_contracts, address, balance=ETHERBASE_ALLOC, code=code)
        else:
            add_to_accounts(predeployed_contracts, address, code=code)
    return predeployed_contracts


def set_contract_verified(schain_name, address):
    config = get_schain_config(schain_name)
    config['verification_status'][address] = True
    write_schain_config(schain_name, config)
