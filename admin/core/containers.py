import logging
import docker
import socket
from contextlib import closing

from admin.utils.logger import init_logger

init_logger()
logger = logging.getLogger(__name__)
dutils = docker.DockerClient()


CONTAINER_NOT_FOUND = 'not_found'
EXITED_STATUS = 'exited'
CREATED_STATUS = 'created'
RUNNING_STATUS = 'running'


def is_explorer_found(schain_name):
    container_name = f'{schain_name}_proxy'
    return is_container_exists(container_name)


def is_explorer_running(schain_name):
    container_name = f'{schain_name}_proxy'
    return get_info(container_name) == RUNNING_STATUS


def remove_explorer(schain_name):
    container_name = f'{schain_name}_proxy'
    if is_container_exists(container_name):
        logger.warning(f'Removing {container_name}...')
        return dutils.containers.get(container_name).remove(force=True)


def is_container_exists(name: str) -> bool:
    try:
        dutils.containers.get(name)
    except docker.errors.NotFound:
        return False
    return True


def get_info(container_id: str):
    try:
        container = dutils.containers.get(container_id)
        return container.status
    except docker.errors.NotFound:
        logger.warning(
            f'Can not get info - no such container: {container_id}')
        return CONTAINER_NOT_FOUND


def get_db_port(schain_name):
    try:
        db = dutils.containers.get(f'{schain_name}_db')
        return get_container_host_port(db)
    except docker.errors.NotFound:
        return get_free_port()


def get_container_host_port(container):
    ports = list(container.attrs['NetworkSettings']['Ports'].values())
    return ports[0][0]['HostPort']


def restart_nginx():
    nginx = dutils.containers.get('nginx')
    logger.info('Restarting nginx container...')
    nginx.exec_run('nginx -s reload')


def restart_postgres(schain_name):
    try:
        db = dutils.containers.get(f'{schain_name}_db')
        logger.info(f'Restarting {schain_name}_db container...')
        db.restart()
    except docker.errors.NotFound:
        logger.warning(f'DB for {schain_name} not found')


def get_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def check_db_exists(schain_name):
    try:
        dutils.containers.get(f'{schain_name}_db')
        return True
    except docker.errors.NotFound:
        return False


def check_db_running(schain_name):
    container_name = f'{schain_name}_db'
    return get_info(container_name) == RUNNING_STATUS
