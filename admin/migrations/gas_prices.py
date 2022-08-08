import logging
import csv
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

from admin import GAS_PRICES_FILEPATH
from admin.configs.meta import get_schain_meta
from admin.utils.helper import read_json

logger = logging.getLogger(__name__)


def upgrade_gas_prices(schain_name):
    logger.info(f'Running gas_prices upgrade for {schain_name}')
    schain_meta = get_schain_meta(schain_name)
    conn = psycopg2.connect(
        host="localhost",
        database="explorer",
        user="postgres",
        port=schain_meta['db_port'])

    cursor = conn.cursor()
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS gas_prices (
          id SERIAL NOT NULL,
          date date NOT NULL,
          gas_price numeric(100,0) NOT NULL
        )
    '''
    cursor.execute(create_table_query)
    conn.commit()

    select_max_date = '''
        select max(date) max_date from gas_prices
    '''
    cursor.execute(select_max_date)
    max_previous_date = cursor.fetchone()[0]
    results = []
    data = read_json(GAS_PRICES_FILEPATH)
    for row in data:
        if not max_previous_date or \
                datetime.strptime(row['UTCDate'], '%Y-%m-%d').date() > max_previous_date:
            results.append((row['UTCDate'], row['avgGasPrice_Wei']))

    insert_query = '''
            INSERT into gas_prices (date, gas_price) VALUES %s
        '''
    execute_values(cursor, insert_query, results)
    conn.commit()


def update_schains_gas_prices(schains_list):
    is_all_updated = True
    for schain in schains_list:
        try:
            upgrade_gas_prices(schain)
            logger.info(f'sChain {schain} upgraded')
        except Exception as e:
            logger.warning(f'Failed to upgrade {schain} gas_prices: {e}')
            is_all_updated = False
    return is_all_updated
