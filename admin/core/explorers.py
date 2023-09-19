import logging
import os
import subprocess

from admin import (DOCKER_COMPOSE_CONFIG_PATH, DOCKER_COMPOSE_BIN_PATH,
                   COMPOSE_HTTP_TIMEOUT, BLOCKSCOUT_DATA_DIR)
from admin.configs.meta import (update_meta_data, get_schain_meta, get_explorers_meta,
                                set_schain_upgraded, is_schain_upgraded, verified_contracts)
from admin.configs.nginx import regenerate_nginx_config
from admin.configs.schains import generate_config
from admin.core.containers import (get_free_port, restart_nginx,
                                   is_explorer_running, remove_explorer)
from admin.core.endpoints import is_dkg_passed, get_schain_endpoint, get_first_block
from admin.core.verify import verify
from admin.migrations.revert_reasons import upgrade

logger = logging.getLogger(__name__)


def run_explorer(schain_name, endpoint, ws_endpoint):
    schain_meta = get_schain_meta(schain_name)
    explorer_port = schain_meta['port'] if schain_meta else get_free_port()
    db_port = schain_meta['db_port'] if schain_meta else get_free_port()
    scv_port = schain_meta['scv_port'] if schain_meta else get_free_port()
    first_block = schain_meta['first_block'] if schain_meta else get_first_block(schain_name)
    config_host_path = generate_config(schain_name)
    blockscout_data_dir = f'{BLOCKSCOUT_DATA_DIR}/{schain_name}'
    env = {
        'SCHAIN_NAME': schain_name,
        'PORT': str(explorer_port),
        'DB_PORT': str(db_port),
        'SC_VERIFIER_PORT': str(scv_port),
        'ENDPOINT': endpoint,
        'WS_ENDPOINT': ws_endpoint,
        'CONFIG_PATH': config_host_path,
        'COMPOSE_PROJECT_NAME': schain_name,
        'COMPOSE_HTTP_TIMEOUT': str(COMPOSE_HTTP_TIMEOUT),
        'FIRST_BLOCK': str(first_block),
        'SCHAIN_DATA_DIR': blockscout_data_dir
    }
    logger.info(f'Running explorer with {env}')
    command = [
        DOCKER_COMPOSE_BIN_PATH,
        '-f',
        DOCKER_COMPOSE_CONFIG_PATH,
        'up',
        '-d'
    ]
    subprocess.run(command, env={**env, **os.environ})
    update_meta_data(schain_name, explorer_port, db_port, scv_port,
                     endpoint, ws_endpoint, first_block)
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


def check_explorer_for_schain(schain_name):
    explorers = get_explorers_meta()
    if schain_name not in explorers and not is_dkg_passed(schain_name):
        return
    if schain_name not in explorers:
        run_explorer_for_schain(schain_name)
        set_schain_upgraded(schain_name)
    if not is_explorer_running(schain_name):
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
