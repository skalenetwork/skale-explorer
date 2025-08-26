import json
import logging
import os
from time import sleep

import requests

from admin import (BLOCKSCOUT_DATA_DIR, ENVS_DIR_PATH, BLOCKSCOUT_PROXY_CONFIG_DIR,
                   SSL_ENABLED, RE_CAPTCHA_SECRET_KEY, NOVES_SUPPORTED_CHAINS,
                   HOST_DOMAIN, BLOCKSCOUT_PROXY_SSL_CONFIG_DIR, HOST_SSL_DIR_PATH,
                   WALLET_CONNECT_PROJECT_ID, BLOCKSCOUT_BACKEND_DOCKER_TAG,
                   BLOCKSCOUT_FRONTEND_DOCKER_TAG, IS_TESTNET, DB_PASSWORD,
                   NOVES_API_KEY, PUBLIC_IP, NETWORK_NAME)
from admin.configs.meta import get_explorer_endpoint
from admin.configs.nginx import regenerate_nginx_config
from admin.configs.schains import generate_config
from admin.core.containers import (restart_nginx,
                                   is_explorer_running, run_blockscout_containers,
                                   stop_blockscout_containers)
from admin.core.endpoints import is_dkg_passed, get_schain_endpoint, get_chain_id
from admin.core.verify import verify
from admin.utils.helper import find_sequential_free_ports, write_json_into_env

logger = logging.getLogger(__name__)


def check_explorer_for_schain(schain_name, update=False):
    if not is_dkg_passed(schain_name):
        return
    if not is_explorer_running(schain_name):
        run_explorer_for_schain(schain_name, update)


def run_explorer_for_schain(schain_name, update=False):
    env_file_path = os.path.join(ENVS_DIR_PATH, f'{schain_name}.env')
    if not os.path.exists(env_file_path) or update:
        env_data = generate_blockscout_envs(schain_name)
        write_json_into_env(env_file_path, env_data)
        logger.info(f'Env for {schain_name} is generated: {env_file_path}')
    run_blockscout_containers(env_file_path)
    regenerate_nginx_config()
    restart_nginx()
    internal_endpoint = get_explorer_endpoint(schain_name)
    logger.info(f'{schain_name} explorer is running on {internal_endpoint} endpoint internally')
    logger.info(f'{schain_name} explorer is running on {schain_name}. subdomain')


def stop_explorer_for_schain(schain_name):
    env_file_path = os.path.join(ENVS_DIR_PATH, f'{schain_name}.env')
    stop_blockscout_containers(env_file_path)
    logger.info('sChain explorer is stopped')


def restart_explorer_for_schain(schain_name):
    env_file_path = os.path.join(ENVS_DIR_PATH, f'{schain_name}.env')
    if not os.path.exists(env_file_path):
        env_data = generate_blockscout_envs(schain_name)
        write_json_into_env(env_file_path, env_data)
        logger.info(f'Env for {schain_name} is generated: {env_file_path}')
    run_blockscout_containers(env_file_path)
    logger.info('sChain explorer is restarted')


def generate_blockscout_envs(schain_name):
    port_envs = generate_port_envs()
    network_envs = generate_network_envs()
    schain_envs = generate_schain_envs(schain_name)
    common_envs = generate_common_envs(schain_name)
    return {
        **port_envs,
        **schain_envs,
        **common_envs,
        **network_envs
    }


def generate_port_envs():
    base_port = find_sequential_free_ports(5)
    return {
        'PROXY_PORT': str(base_port),
        'DB_PORT': str(base_port + 1),
        'STATS_PORT': str(base_port + 2),
        'STATS_DB_PORT': str(base_port + 3),
    }


def generate_common_envs(schain_name):
    common_envs = {
        'COMPOSE_PROJECT_NAME': schain_name,
        'BLOCKSCOUT_BACKEND_DOCKER_TAG': BLOCKSCOUT_BACKEND_DOCKER_TAG,
        'BLOCKSCOUT_FRONTEND_DOCKER_TAG': BLOCKSCOUT_FRONTEND_DOCKER_TAG,
    }
    if WALLET_CONNECT_PROJECT_ID:
        common_envs.update({
            'NEXT_PUBLIC_WALLET_CONNECT_PROJECT_ID': WALLET_CONNECT_PROJECT_ID
        })
    if DB_PASSWORD:
        common_envs.update({
            'DB_PASSWORD': DB_PASSWORD
        })
    if RE_CAPTCHA_SECRET_KEY:
        common_envs.update({
            'RE_CAPTCHA_SECRET_KEY': RE_CAPTCHA_SECRET_KEY
        })
    return common_envs


def generate_schain_envs(schain_name):
    network = 'testnet' if IS_TESTNET is True else 'mainnet'
    chains_metadata_url = \
        f'https://raw.githubusercontent.com/skalenetwork/skale-network/master/metadata/{network}/chains.json' # noqa
    try:
        schain_app_name = requests.get(chains_metadata_url).json()[schain_name]['alias']
    except KeyError:
        schain_app_name = schain_name
    if NETWORK_NAME == 'fair' and network == 'testnet':
        schain_app_name = 'FAIR Testnet'
    else :
        schain_app_name = 'FAIR'
    config_host_path = generate_config(schain_name)
    schain_data_dir = f'{BLOCKSCOUT_DATA_DIR}/{schain_name}'
    schain_envs = {
        'SCHAIN_NAME': schain_name,
        'SCHAIN_APP_NAME': schain_app_name,
        'CHAIN_ID': str(get_chain_id(schain_name)),
        'ENDPOINT': get_schain_endpoint(schain_name),
        'WS_ENDPOINT': get_schain_endpoint(schain_name, ws=True),
        'SCHAIN_DATA_DIR': schain_data_dir,
        'CONFIG_PATH': config_host_path,
        'NEXT_PUBLIC_IS_TESTNET': json.dumps(IS_TESTNET),
        'NEXT_PUBLIC_TRANSACTION_INTERPRETATION_PROVIDER': 'none'
    }
    if NOVES_SUPPORTED_CHAINS.get(schain_name):
        schain_envs.update({
            'NOVES_FI_CHAIN_NAME': NOVES_SUPPORTED_CHAINS[schain_name],
            'NOVES_API_KEY': NOVES_API_KEY,
            'NEXT_PUBLIC_TRANSACTION_INTERPRETATION_PROVIDER': 'noves'
        })
    return schain_envs


def generate_network_envs():
    if SSL_ENABLED:
        return {
            'HOST': HOST_DOMAIN,
            'PROXY_BASE_PORT': str(443),
            'SSL_ENABLED': 'true',
            'BLOCKSCOUT_PROXY_CERTS_PATH': HOST_SSL_DIR_PATH,
            'BLOCKSCOUT_PROXY_CONFIG_DIR': BLOCKSCOUT_PROXY_SSL_CONFIG_DIR
        }
    public_ip = requests.get('https://api.ipify.org').content.decode('utf8')
    if PUBLIC_IP:
        public_ip = PUBLIC_IP
    return {
        'HOST': public_ip,
        'BLOCKSCOUT_PROXY_CONFIG_DIR': BLOCKSCOUT_PROXY_CONFIG_DIR
    }
