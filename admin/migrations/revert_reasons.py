import logging

import psycopg2
from psycopg2.extras import execute_values
from web3 import HTTPProvider, Web3
from admin.configs.meta import set_schain_upgraded, get_schain_meta

logger = logging.getLogger(__name__)


def upgrade_revert_reasons(schain_name):
    logger.info(f'Running revert_reason upgrade for {schain_name}')
    schain_meta = get_schain_meta(schain_name)
    conn = psycopg2.connect(
        host="localhost",
        database="explorer",
        user="postgres",
        port=schain_meta['db_port'])

    provider = HTTPProvider(schain_meta['endpoint'])
    web3 = Web3(provider)
    cursor = conn.cursor()
    limit_number = 1000
    select_query = f"""
        SELECT hash,status,revert_reason,block_number
        FROM transactions
        WHERE status=0 AND revert_reason is null
        ORDER BY block_number DESC LIMIT {limit_number};
    """
    cursor.execute(select_query)

    data = cursor.fetchall()
    logger.info(f'Found {len(data)} txs to be checked')
    data_to_update = []
    for i in data:
        hash = bytes(i[0]).hex()
        try:
            receipt = web3.eth.get_transaction_receipt(hash)
            if receipt.get('revertReason'):
                data_to_update.append((hash, receipt.revertReason))
        except Exception:
            continue

    if data_to_update:
        logger.info(f'Updating {len(data_to_update)} txs')
        update_query = """UPDATE transactions AS t
                          SET revert_reason = e.revert_reason
                          FROM (VALUES %s) AS e(hash, revert_reason)
                          WHERE decode(e.hash, 'hex') = t.hash;"""
        execute_values(cursor, update_query, data_to_update)
        conn.commit()


def upgrade(schain_name):
    try:
        upgrade_revert_reasons(schain_name)
        set_schain_upgraded(schain_name)
        logger.info(f'sChain {schain_name} upgraded')
    except Exception as e:
        logger.warning(f'Failed to upgrade {schain_name}: {e}')
