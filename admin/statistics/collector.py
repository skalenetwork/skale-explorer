import logging

import psycopg2
from admin.configs.meta import get_schain_meta

logger = logging.getLogger(__name__)


def collect_stats(schain_name):
    schain_meta = get_schain_meta(schain_name)
    conn = psycopg2.connect(
        host="localhost",
        database="explorer",
        user="postgres",
        port=schain_meta['db_port'])

    query_1 = '''
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

    cursor = conn.cursor()
    cursor.execute(query_1)
    return cursor.fetchall()