import logging
import psycopg2
from psycopg2.extras import RealDictCursor

from admin.configs.meta import get_schain_meta


logger = logging.getLogger(__name__)


def collect_schain_stats(schain_name):
    schain_meta = get_schain_meta(schain_name)
    conn = psycopg2.connect(
        host="localhost",
        database="explorer",
        user="postgres",
        port=schain_meta['db_port'])

    txs_query = '''
    SELECT
        count(case when (NOW()::date-inserted_at::date) < 7 THEN 1 else null end) tx_count_7_days,
        count(DISTINCT case when (NOW()::date-inserted_at::date) < 7 THEN hash else null end) unique_tx_count_7_days,
        count(DISTINCT case when (NOW()::date-inserted_at::date) < 7 THEN from_address_hash else null end) user_count_7_days,
        sum(DISTINCT case when (NOW()::date-inserted_at::date) < 7 THEN gas_used else 0 end) gas_total_used_7_days,
        count(DISTINCT case when (NOW()::date-inserted_at::date) < 7 THEN from_address_hash else null end) user_count_7_days_gwei,
        sum(DISTINCT case when (NOW()::date-inserted_at::date) < 7 THEN gas_used else 0 end) / 1000000000 gas_total_used_7_days_eth ,
        count(case when (NOW()::date-inserted_at::date) < 30 THEN 1 else null end) tx_count_30_days,
        count(DISTINCT case when (NOW()::date-inserted_at::date) < 30 THEN hash else null end) unique_tx_count_30_days,
        count(DISTINCT case when (NOW()::date-inserted_at::date) < 30 THEN from_address_hash else null end) user_count_30_days,
        sum(DISTINCT case when (NOW()::date-inserted_at::date) < 30 THEN gas_used else 0 end) gas_total_used_30_days_gwei,
        sum(DISTINCT case when (NOW()::date-inserted_at::date) < 30 THEN gas_used else 0 end) / 1000000000 gas_total_used_30_days_eth
    FROM transactions
    '''

    txs_day_query = '''
    SELECT
        count(1) tx_count,
        count(DISTINCT hash) unique_tx,
        count(DISTINCT from_address_hash) user_count,
        sum(DISTINCT gas_used) gas_total_used_gwei,
        sum(DISTINCT gas_used) / 1000000000 gas_total_used_ETH,
        TO_CHAR(inserted_at :: DATE, 'yyyymmdd') as TX_DATE
    FROM transactions
    where NOW()::date-inserted_at::date < 7
    GROUP by TO_CHAR(inserted_at :: DATE, 'yyyymmdd')
    '''

    blocks_query = '''
    SELECT
        count(case when (NOW()::date-timestamp::date) <= 0 THEN 1 else null end) block_count_24_hour,
        count(case when (NOW()::date-timestamp::date) < 7 THEN 1 else null end) block_count_7_days,
        count(case when (NOW()::date-timestamp::date) < 30 THEN 1 else null end) block_count_30_days
    FROM blocks
    '''

    tps_query = '''
    SELECT MAX(cnt_per_second) max_tps_last_7_days
    from (
        SELECT count(1) cnt_per_second,
        TO_CHAR(inserted_at :: DATE, 'YYYY-MM-DD HH:MM:SS')
        from transactions
        WHERE
        NOW()::date-inserted_at::date < 7
        group by
        TO_CHAR(inserted_at :: DATE, 'YYYY-MM-DD HH:MM:SS')
    )
    '''

    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(txs_query)
    txs_res = cursor.fetchall()
    cursor.execute(txs_day_query)
    txs_day_res = cursor.fetchall()
    cursor.execute(blocks_query)
    blocks_res = cursor.fetchall()
    cursor.execute(tps_query)
    tps_res = cursor.fetchall()

    # logger.info(f'Txs: {dict(txs_res[0])}')
    # logger.info(f'Txs day: {dict(txs_day_res[0])}')
    # logger.info(f'Blocks: {dict(blocks_res[0])}')
    return dict(txs_res[0])
