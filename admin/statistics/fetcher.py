import requests
import logging
from datetime import datetime
from admin.statistics.database import NetworkStatsRecord

logger = logging.getLogger(__name__)


def collect_covalent_stats():
    txs_montly_raw = get_covalent_data('3a48d6b4c58c465e9780f30192b')
    gas_monthly_raw = get_covalent_data('3375f472e34b463d906f70cf762')
    txs_30d_raw = get_covalent_data('d0134db54eeb4bee886c9834d69')

    txs_montly = get_data_dict(txs_montly_raw, 'transactions')
    gas_montly = get_data_dict(gas_monthly_raw, 'gas_used')
    txs_30d = txs_30d_raw[0]['amount_transactions']

    result = {}
    for key in txs_montly:
        result[key] = {
            'tx_count': txs_montly[key],
            'gas_total_used': gas_montly[key]
        }
    return {
        'group_by_months': result,
        'tx_count_30_days': txs_30d
    }


def get_covalent_data(card_id):
    try:
        url = f'https://api.covalenthq.com/_/embed_card/card_{card_id}/data/?date_aggregation=monthly&date_range=this_month' # noqa
        data = requests.get(url).json()['data']['item']['chart']['chart_data']['data']
        return data
    except ConnectionError:
        logger.error(f'Could not fetch covalent data for {card_id}')


def get_data_dict(data, value_key):
    res = {}
    for data_item in data:
        date = datetime.strptime(data_item['date'], '%Y-%m-%d').strftime('%Y-%m')
        value = 0 if data_item.get(value_key) is None else int(data_item[value_key])
        if res.get(date):
            res[date] += value
        else:
            res[date] = value
    return res


def collect_global_stats():
    bs_stats = NetworkStatsRecord.get_last_stats()
    covalent_stats = collect_covalent_stats()

    bs_mothly_stats = bs_stats['group_by_months']
    covalent_monthly_stats = covalent_stats['group_by_months']

    if int(bs_stats['tx_count_30_days']) < int(covalent_stats['tx_count_30_days']):
        bs_stats['tx_count_30_days'] = covalent_stats['tx_count_30_days']

    total_txs = 0
    total_unique_txs = 0

    for month_data in bs_mothly_stats:
        date = month_data['tx_date']
        if covalent_monthly_stats.get(date):
            if covalent_monthly_stats[date]['tx_count'] > month_data['tx_count']:
                month_data['tx_count'] = covalent_monthly_stats[date]['tx_count']
                month_data['unique_tx'] = covalent_monthly_stats[date]['tx_count']
            if covalent_monthly_stats[date]['gas_total_used'] > month_data['gas_total_used']:
                month_data['gas_total_used'] = covalent_monthly_stats[date]['gas_total_used']
        total_txs += month_data['tx_count']
        total_unique_txs += month_data['unique_tx']

    bs_mothly_stats['tx_count_total'] = total_txs
    bs_mothly_stats['unique_tx_count_total'] = total_txs
    return bs_stats