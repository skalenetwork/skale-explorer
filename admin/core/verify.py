import logging
from time import sleep

import requests
import json

from web3 import Web3

from admin.configs.meta import get_explorer_endpoint, set_chain_verified
from admin.configs.schains import get_schain_config, set_contract_verified

logger = logging.getLogger(__name__)


def verify(schain_name):
    logger.info(f'Verifying contracts for {schain_name}')
    config = get_schain_config(schain_name)
    verification_data = config['verify']
    for verifying_address in verification_data.keys():
        if not config['verification_status'][verifying_address]:
            verify_contract(schain_name, verifying_address, verification_data[verifying_address])
    all_verified = True
    upd_config = get_schain_config(schain_name)
    for verifying_address in verification_data.keys():
        if not upd_config['verification_status'][verifying_address]:
            logger.info(f'Contract {verifying_address} is not verified')
            all_verified = False
    if all_verified:
        logger.info(f'All contracts are verified for {schain_name}')
        set_chain_verified(schain_name)


def verify_contract(schain_name, verifying_address, contract_meta):
    logging.info(f'Verifying {verifying_address} contract')
    contract = {
        'contractaddress': verifying_address,
        'contractname': contract_meta['name'],
        'compilerversion': f'v{contract_meta["solcLongVersion"]}',
        'sourceCode': json.dumps(contract_meta['input'])
    }
    response = send_verify_request(schain_name, contract)
    if response and check_verify_status(schain_name, response['result']):
        set_contract_verified(schain_name, verifying_address)


def get_verified_contract_list(schain_name):
    schain_explorer_endpoint = get_explorer_endpoint(schain_name)
    headers = {'content-type': 'application/json'}
    addresses = []
    try:
        result = requests.get(
            f'{schain_explorer_endpoint}/api?module=contract&action=listcontracts&filter=verified',
            headers=headers
        ).json()['result']
        addresses = [Web3.toChecksumAddress(contract['Address']) for contract in result]
    except requests.exceptions.ConnectionError as e:
        logger.warning(f'get_contract_list failed with {e}')
    return addresses


def get_veify_url(schain_name):
    schain_explorer_endpoint = get_explorer_endpoint(schain_name)
    return f'{schain_explorer_endpoint}/api?module=contract&action=verifysourcecode&codeformat=solidity-standard-json-input'


def send_verify_request(schain_name, verification_data):
    headers = {'content-type': 'application/json'}
    try:
        return requests.post(
            get_veify_url(schain_name),
            data=json.dumps(verification_data),
            headers=headers
        ).json()
    except requests.exceptions.ConnectionError as e:
        logger.warning(f'verifying_address failer with {e}')


def is_contract_verified(schain_name, address):
    schain_explorer_endpoint = get_explorer_endpoint(schain_name)
    headers = {'content-type': 'application/json'}
    try:
        result = requests.get(
            f'{schain_explorer_endpoint}/api?module=contract&action=getabi&address={address}',
            headers=headers
        ).json()['status']
        return False if int(result) == 0 else True
    except requests.exceptions.ConnectionError as e:
        logger.warning(f'is_contract_verified failed with {e}')


def check_verify_status(schain_name, uid):
    if uid == 'Smart-contract already verified':
        logger.info('Contract already verified')
        return True
    schain_explorer_endpoint = get_explorer_endpoint(schain_name)
    headers = {'content-type': 'application/json'}
    try:
        while True:
            url = f'{schain_explorer_endpoint}/api?module=contract&action=checkverifystatus&guid={uid}'
            response = requests.get(
                url,
                headers=headers
            ).json()
            if response['result'] == 'Pending in queue' or response['result'] == 'Unknown UID':
                logger.info(f'Verify status: {response["result"]}...')
                sleep(10)
            else:
                if response['result'] == 'Pass - Verified':
                    logger.info('Contract successfully verified')
                    return True
                elif response['result'] == 'Fail - Unable to verify':
                    logger.info('Failed to verified contract')
                else:
                    logger.info(response['result'])
                break
    except requests.exceptions.ConnectionError as e:
        logger.warning(f'checkverifystatus failed with {e}')
    return False
