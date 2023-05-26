import logging
from datetime import datetime
from decimal import Decimal
from time import time, sleep

import psycopg2
from psycopg2.extras import RealDictCursor

from admin.configs.meta import get_schain_meta
from admin.core.containers import restart_postgres
from admin.statistics.database import SchainStatsRecord, NetworkStatsRecord

logger = logging.getLogger(__name__)


def collect_schain_stats(schain_name):
    schain_meta = get_schain_meta(schain_name)
    if not schain_meta:
        logger.warning(f'Explorer for {schain_name} is not created yet')
        return {}

    connect_creds = {
        'host': 'localhost',
        'database': 'explorer',
        'user': 'postgres',
        'port': schain_meta['db_port'],
        'schain_name': schain_name
    }

    queries = ['''
    SELECT
        count(case when (NOW()::date-blocks.timestamp::date) < 7 THEN 1 else null end) tx_count_7_days,
        count(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 7 THEN transactions.hash else null end) unique_tx_count_7_days,
        count(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 7 THEN from_address_hash else null end) user_count_7_days,
        sum(case when (NOW()::date-blocks.timestamp::date) < 7 THEN transactions.gas_used else 0 end) gas_total_used_7_days,
        sum(case when (NOW()::date-blocks.timestamp::date) < 7 THEN transactions.gas_used else 0 end * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end) / power(10, 9) gas_fees_total_7_days_gwei,
        sum(case when (NOW()::date-blocks.timestamp::date) < 7 THEN transactions.gas_used else 0 end * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end) / power(10, 18) gas_fees_total_7_days_eth,
        sum(case when (NOW()::date-blocks.timestamp::date) < 7 THEN transactions.gas_used else 0 end * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end * (market_history.opening_price + market_history.closing_price) / 2) / power(10, 18) gas_fees_total_7_days_usd,
        
        count(case when (NOW()::date-blocks.timestamp::date) < 30 THEN 1 else null end) tx_count_30_days,
        count(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 30 THEN transactions.hash else null end) unique_tx_count_30_days,
        count(DISTINCT case when (NOW()::date-blocks.timestamp::date) < 30 THEN from_address_hash else null end) user_count_30_days,
        sum(case when (NOW()::date-blocks.timestamp::date) < 30 THEN transactions.gas_used else 0 end) gas_total_used_30_days,
        sum(case when (NOW()::date-blocks.timestamp::date) < 30 THEN transactions.gas_used else 0 end * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end) / power(10, 9) gas_fees_total_30_days_gwei,
        sum(case when (NOW()::date-blocks.timestamp::date) < 30 THEN transactions.gas_used else 0 end * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end) / power(10, 18) gas_fees_total_30_days_eth,
        sum(case when (NOW()::date-blocks.timestamp::date) < 30 THEN transactions.gas_used else 0 end * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end * (market_history.opening_price + market_history.closing_price) / 2) / power(10, 18) gas_fees_total_30_days_usd,
        
        count(1) tx_count_total,
        count(DISTINCT transactions.hash) unique_tx_count_total,
        count(DISTINCT from_address_hash) user_count_total,
        sum(transactions.gas_used) gas_total_used,
        sum(transactions.gas_used * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end) / power(10, 9) gas_fees_total_gwei,
        sum(transactions.gas_used * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end) / power(10, 18) gas_fees_total_eth,
        sum(transactions.gas_used * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end * (market_history.opening_price + market_history.closing_price) / 2) / power(10, 18) gas_fees_total_usd

    FROM transactions
    inner join blocks on blocks.number = transactions.block_number
    left outer join gas_prices on gas_prices.date::date = blocks.timestamp::date
    left outer join market_history on market_history.date::date = blocks.timestamp::date;
    ''', '''
    SELECT
        count(case when (NOW()::date-timestamp::date) <= 0 THEN 1 else null end) block_count_24_hours,
        count(case when (NOW()::date-timestamp::date) < 7 THEN 1 else null end) block_count_7_days,
        count(case when (NOW()::date-timestamp::date) < 30 THEN 1 else null end) block_count_30_days,
        count(1) block_count_total
    FROM (select distinct timestamp, number from blocks) blocks
    ''', '''
    SELECT MAX(cnt_per_second) max_tps_last_24_hours
    from (
        SELECT 
        blocks.timestamp, 
        count(1) / extract(epoch from (blocks.timestamp - LAG (blocks.timestamp,1) OVER (ORDER BY blocks.timestamp ASC))) AS cnt_per_second
        from transactions
        inner join blocks on blocks.number = transactions.block_number
        WHERE NOW()::date-blocks.timestamp::date < 1
        group by blocks.timestamp
    ) as foo;
    ''', ''' 
    SELECT MAX(cnt_per_second) max_tps_last_7_days
    from (
        SELECT 
        blocks.timestamp, 
        count(1) / extract(epoch from (blocks.timestamp - LAG (blocks.timestamp,1) OVER (ORDER BY blocks.timestamp ASC))) AS cnt_per_second
        from transactions
        inner join blocks on blocks.number = transactions.block_number
        WHERE NOW()::date-blocks.timestamp::date < 7
        group by blocks.timestamp
    ) as foo;
    ''', '''
    SELECT MAX(cnt_per_second) max_tps_last_30_days
    from (
        SELECT 
        blocks.timestamp, 
        count(1) / extract(epoch from (blocks.timestamp - LAG (blocks.timestamp,1) OVER (ORDER BY blocks.timestamp ASC))) AS cnt_per_second
        from transactions
        inner join blocks on blocks.number = transactions.block_number
        WHERE NOW()::date-blocks.timestamp::date < 30
        group by blocks.timestamp
    ) as foo;
    ''']
    multi_queries = {
        'data_by_days': '''
            SELECT
                count(1) tx_count,
                count(DISTINCT transactions.hash) unique_tx,
                count(DISTINCT from_address_hash) user_count,
                sum(transactions.gas_used) gas_total_used,
                sum(transactions.gas_used * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end) / power(10, 9) gas_fees_total_gwei,
                sum(transactions.gas_used * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end) / power(10, 18) gas_fees_total_eth,
                sum(transactions.gas_used * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end * (market_history.opening_price + market_history.closing_price)) / (2 * power(10, 18)) gas_fees_total_usd,
                TO_CHAR(blocks.timestamp :: DATE, 'YYYY-MM-DD') as TX_DATE
            FROM transactions
            inner join blocks on blocks.number = transactions.block_number
            left outer join market_history on market_history.date::date = blocks.timestamp::date
            left outer join gas_prices on gas_prices.date::date = blocks.timestamp::date
            where NOW()::date-blocks.timestamp::date < 7
            GROUP by TO_CHAR(blocks.timestamp :: DATE, 'YYYY-MM-DD')
            ''',
        'data_by_months': '''
            SELECT
                count(1) tx_count,
                count(DISTINCT transactions.hash) unique_tx,
                count(DISTINCT from_address_hash) user_count,
                sum(transactions.gas_used) gas_total_used,
                sum(transactions.gas_used * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end) / power(10, 9) gas_fees_total_gwei,
                sum(transactions.gas_used * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end) / power(10, 18) gas_fees_total_eth,
                sum(transactions.gas_used * case when gas_prices.gas_price is not null THEN gas_prices.gas_price else (select gas_price from gas_prices order by date desc limit 1) end * (market_history.opening_price + market_history.closing_price)) / (2 * power(10, 18)) gas_fees_total_usd,
                TO_CHAR(blocks.timestamp :: DATE, 'YYYY-MM') as TX_DATE
            FROM blocks
            inner join transactions on blocks.number = transactions.block_number
            left outer join market_history on market_history.date::date = blocks.timestamp::date
            left outer join gas_prices on gas_prices.date::date = blocks.timestamp::date
            GROUP by TO_CHAR(blocks.timestamp :: DATE, 'YYYY-MM')
            '''
    }

    raw_result = {}
    for query in queries:
        result = execute_query(query, **connect_creds)
        if result['status'] == 0 and len(result['data']):
            raw_result.update(dict(result['data'][0]))

    raw_result_multi = []
    for query in multi_queries:
        result = execute_query(multi_queries[query], **connect_creds)
        if result['status'] == 0:
            for data in result['data']:
                raw_data = dict(data)
                if query == 'data_by_days':
                    raw_data.update({'data_by_days': True})
                else:
                    raw_data.update({'data_by_days': False})
                raw_result_multi.append(raw_data)

    if raw_result.get('tx_date'):
        raw_result.pop('tx_date')
    result = {
        key: float(raw_result[key]) if type(raw_result[key]) == Decimal else raw_result[key]
        for key in raw_result
        if raw_result[key] is not None
    }
    result['group_stats'] = raw_result_multi
    return result


def update_schains_stats(schain_names):
    total_stats = {}
    for schain in schain_names:
        logger.info(f'Collecting stats for {schain}...')
        schain_stats = collect_schain_stats(schain)
        refill_schain_stats(schain, schain_stats)
        logger.info(f'Stats for {schain}: {schain_stats}')
        SchainStatsRecord.add(
            schain_name=schain,
            inserted_at=datetime.fromtimestamp(time()),
            **schain_stats
        )
        update_total_dict(total_stats, schain_stats)
    logger.info(f'Schains: {len(schain_names)}; total stats: {total_stats}')
    timestamp = time()
    NetworkStatsRecord.add(
        schains_number=len(schain_names),
        inserted_at=datetime.fromtimestamp(time()),
        **total_stats
    )
    return timestamp


def refill_schain_stats(schain, schain_stats):
    cached = SchainStatsRecord.get_last_cached_stats(schain)
    if cached is None:
        return
    for key in cached:
        if schain_stats.get(key) is None:
            cached_value = cached[key]
            logger.warning(f'Key {key} not found, using cached value: {cached_value}')
            schain_stats[key] = cached_value
        if key == 'group_stats':
            for sample_cached in cached[key]:
                is_find = False
                for sample in schain_stats[key]:
                    if sample_cached['tx_date'] == sample['tx_date']:
                        is_find = True
                        break
                if not is_find:
                    logger.warning(f'{sample_cached["tx_date"]} not found, '
                                   f'using cached value: {sample_cached}')
                    schain_stats[key].append(sample_cached)


def execute_query(query, **connection_creds):
    attempts = 0
    schain_name = connection_creds.pop('schain_name')
    while attempts < 3:
        try:
            with psycopg2.connect(**connection_creds) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query)
                    return {
                        'status': 0,
                        'data': cursor.fetchall()
                    }
        except Exception as e:
            logger.warning(f'Query failed: {e}')
            restart_postgres(schain_name)
            sleep(60)
            attempts += 1
    return {
        'status': 1,
        'data': None
    }


def update_total_dict(total_stats, schain_stats):
    for key in schain_stats:
        if key.startswith('max'):
            total_stats[key] = max(total_stats.get(key, 0), schain_stats[key])
        elif key.startswith('group_stats'):
            if not total_stats.get(key):
                total_stats[key] = []
            for sample in schain_stats[key]:
                is_find = False
                for sample_total in total_stats[key]:
                    if sample_total['tx_date'] == sample['tx_date']:
                        for metric in sample:
                            if metric != 'tx_date' and metric != 'data_by_days' and \
                                    sample[metric] is not None:
                                sample_total[metric] += sample[metric]
                        is_find = True
                        break
                if not is_find:
                    total_stats[key].append(sample)
        else:
            total_stats[key] = total_stats.get(key, 0) + schain_stats[key]
    return total_stats
