import logging
from datetime import datetime
from decimal import Decimal
from time import time

import psycopg2
from psycopg2.extras import RealDictCursor

from admin.configs.meta import get_schain_meta
from admin.statistics.database import StatsRecord, GroupStats, create_tables

logger = logging.getLogger(__name__)


def collect_schain_stats(schain_name):
    schain_meta = get_schain_meta(schain_name)
    if not schain_meta:
        logger.warning(f'Explorer for {schain_name} is not created yet')
        return {}

    conn = psycopg2.connect(
        host="localhost",
        database="explorer",
        user="postgres",
        port=schain_meta['db_port'])

    queries = ['''
    SELECT
        count(case when (NOW()::date-blocks.timestamp::date) < 7 THEN 1 else null end) tx_count_7_days,
        count(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 7 THEN transactions.hash else null end) unique_tx_count_7_days,
        count(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 7 THEN from_address_hash else null end) user_count_7_days,
        sum(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 7 THEN transactions.gas_used else 0 end) gas_total_used_7_days_gwei,
        sum(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 7 THEN transactions.gas_used else 0 end) / 1000000000 gas_total_used_7_days_eth ,
        count(case when (NOW()::date-blocks.timestamp::date) < 30 THEN 1 else null end) tx_count_30_days,
        count(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 30 THEN transactions.hash else null end) unique_tx_count_30_days,
        count(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 30 THEN from_address_hash else null end) user_count_30_days,
        sum(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 30 THEN transactions.gas_used else 0 end) gas_total_used_30_days_gwei,
        sum(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 30 THEN transactions.gas_used else 0 end) / 1000000000 gas_total_used_30_days_eth
    FROM transactions
    inner join blocks on blocks.number = transactions.block_number
    ''', '''
    SELECT 
        count(1) tx_count_24_hours,
        count(DISTINCT transactions.hash) unique_tx_24_hours,
        count(DISTINCT from_address_hash) user_count_24_hours, 
        sum(DISTINCT transactions.gas_used) gas_total_used_24_hours_gwei, 
        sum(DISTINCT transactions.gas_used) / 1000000000 gas_total_used_24_hours_eth,
        TO_CHAR(blocks.timestamp :: DATE, 'yyyymmdd') as TX_DATE
    FROM transactions
    inner join blocks on blocks.number = transactions.block_number
    where NOW()::date-blocks.timestamp::date < 7
    GROUP by TO_CHAR(blocks.timestamp :: DATE, 'yyyymmdd')
    ''', '''
    SELECT
        count(case when (NOW()::date-timestamp::date) <= 0 THEN 1 else null end) block_count_24_hours,
        count(case when (NOW()::date-timestamp::date) < 7 THEN 1 else null end) block_count_7_days,
        count(case when (NOW()::date-timestamp::date) < 30 THEN 1 else null end) block_count_30_days,
        count(1) block_count_total
    FROM blocks
    ''', '''
        SELECT MAX(cnt_per_second) max_tps_last_24_hours
        from (
          SELECT 
          blocks.timestamp, 
          count(1) / extract(epoch from (blocks.timestamp - LAG (blocks.timestamp,1) OVER (ORDER BY blocks.timestamp ASC))) AS cnt_per_second
          from transactions
          inner join blocks on blocks.number = transactions.block_number
          WHERE
            NOW()::date-blocks.timestamp::date < 1
          group by 
            blocks.timestamp
        ) as foo;
    ''', ''' 
        SELECT MAX(cnt_per_second) max_tps_last_7_days
        from (
          SELECT 
          blocks.timestamp, 
          count(1) / extract(epoch from (blocks.timestamp - LAG (blocks.timestamp,1) OVER (ORDER BY blocks.timestamp ASC))) AS cnt_per_second
          from transactions
          inner join blocks on blocks.number = transactions.block_number
          WHERE
            NOW()::date-blocks.timestamp::date < 7
          group by 
            blocks.timestamp
        ) as foo;
    ''', '''
        SELECT MAX(cnt_per_second) max_tps_last_30_days
        from (
          SELECT 
          blocks.timestamp, 
          count(1) / extract(epoch from (blocks.timestamp - LAG (blocks.timestamp,1) OVER (ORDER BY blocks.timestamp ASC))) AS cnt_per_second
          from transactions
          inner join blocks on blocks.number = transactions.block_number
          WHERE
            NOW()::date-blocks.timestamp::date < 30
          group by 
            blocks.timestamp
        ) as foo;
    ''', '''
        SELECT 
            count(distinct hash) tx_count_total, 
            count(distinct from_address_hash) user_count_total
        from transactions
    ''']
    multi_queries = {
        'data_by_days': '''
            SELECT
                count(1) tx_count,
                count(DISTINCT transactions.hash) unique_tx,
                count(DISTINCT from_address_hash) user_count,
                sum(DISTINCT transactions.gas_used) gas_total_used_gwei,
                sum(DISTINCT transactions.gas_used) / 1000000000 gas_total_used_ETH,
                TO_CHAR(blocks.timestamp :: DATE, 'YYYY-MM-DD') as TX_DATE
            FROM transactions
            inner join blocks on blocks.number = transactions.block_number
            where NOW()::date-blocks.timestamp::date < 7
            GROUP by TO_CHAR(blocks.timestamp :: DATE, 'YYYY-MM-DD')
            ''',
        'data_by_months': '''
            SELECT
                count(1) tx_count,
                count(DISTINCT transactions.hash) unique_tx,
                count(DISTINCT from_address_hash) user_count,
                sum(DISTINCT transactions.gas_used) gas_total_used_gwei,
                sum(DISTINCT transactions.gas_used) / 1000000000 gas_total_used_ETH,
                TO_CHAR(blocks.timestamp :: DATE, 'YYYY-MM') as TX_DATE
            FROM transactions
            inner join blocks on blocks.number = transactions.block_number
            GROUP by TO_CHAR(blocks.timestamp :: DATE, 'YYYY-MM')
            '''
    }

    raw_result = {}
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    for query in queries:
        try:
            cursor.execute(query)
            raw_result.update(dict(cursor.fetchall()[0]))
        except Exception as e:
            logger.warning(f'Query failed: {e}')
            continue

    raw_result_multi = []
    for query in multi_queries:
        try:
            cursor.execute(multi_queries[query])
            for data in cursor.fetchall():
                raw_data = dict(data)
                if query == 'data_by_days':
                    raw_data.update({'data_by_days': True})
                else:
                    raw_data.update({'data_by_days': False})
                raw_result_multi.append(raw_data)
        except Exception as e:
            logger.warning(f'Query failed: {e}')
            continue

    # print(raw_result_multi)
    if raw_result.get('tx_date'):
        raw_result.pop('tx_date')
    result = {
        key: float(raw_result[key]) if type(raw_result[key]) == Decimal else raw_result[key]
        for key in raw_result
        if raw_result[key] is not None
    }
    result['groups'] = raw_result_multi
    return result


def update_schains_stats(schain_names):
    total_stats = {}
    for schain in schain_names:
        schain_stats = collect_schain_stats(schain)
        logger.info(f'Stats for {schain}: {schain_stats}')
        update_total_dict(total_stats, schain_stats)
    print(total_stats)
    logger.info(f'Schains: {len(schain_names)}; total stats: {total_stats}')
    timestamp = time()
    StatsRecord.add(
        schains_number=len(schain_names),
        inserted_at=datetime.fromtimestamp(timestamp),
        **total_stats
    )
    return timestamp


def update_total_dict(total_stats, schain_stats):
    for key in schain_stats:
        if key.startswith('max'):
            total_stats[key] = max(total_stats.get(key, 0), schain_stats[key])
        elif key.startswith('groups'):
            if not total_stats.get(key):
                total_stats[key] = []
            for sample in schain_stats[key]:
                is_find = False
                for i in total_stats[key]:
                    if i['tx_date'] == sample['tx_date']:
                        for k in sample:
                            if k != 'tx_date' and k != 'data_by_days':
                                i[k] += sample[k]
                        is_find = True
                        break
                if not is_find:
                    total_stats[key].append(sample)
        else:
            total_stats[key] = total_stats.get(key, 0) + schain_stats[key]
    return total_stats
