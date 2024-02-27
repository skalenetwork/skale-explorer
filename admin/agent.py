import logging
import os
from functools import wraps
from threading import Thread
from time import sleep

from admin import EXPLORERS_META_DATA_PATH, ABI_FILEPATH
from admin.configs.meta import (create_meta_file)
from admin.core.endpoints import get_all_names
from admin.core.explorers import check_explorer_for_schain
from admin.utils.logger import init_logger

logger = logging.getLogger(__name__)


def daemon(delay=60):
    def actual_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f'Initiating {func.__name__}')
            while True:
                try:
                    func(*args, **kwargs)
                except Exception as e:
                    logger.exception(f'{func.__name__} failed with: {e}')
                    logger.warning(f'Restarting {func.__name__}...')
                sleep(delay)
        return wrapper
    return actual_decorator


@daemon()
def check_explorer_status():
    schains = get_all_names()
    for schain_name in schains:
        check_explorer_for_schain(schain_name)


def main():
    assert os.path.isfile(ABI_FILEPATH), "ABI not found"
    if not os.path.isfile(EXPLORERS_META_DATA_PATH):
        create_meta_file()

    Thread(target=check_explorer_status, daemon=True, name='explorers-checker').start()
    while True:
        sleep(1)


if __name__ == '__main__':
    init_logger()
    main()
