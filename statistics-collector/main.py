import logging
from time import sleep
from utils import init_logger

logger = logging.getLogger(__name__)


def main():
    sleep_time = 600
    while True:
        logger.info('Collecting stats...')
        logger.info(f'Sleeping {sleep_time}s')
        sleep(sleep_time)


if __name__ == '__main__':
    init_logger()
    main()