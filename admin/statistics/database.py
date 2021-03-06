import logging
from playhouse.shortcuts import model_to_dict
from peewee import (Model, SqliteDatabase, IntegerField, DateTimeField,
                    FloatField, PrimaryKeyField, IntegrityError, DoesNotExist, ForeignKeyField, DateField, BooleanField)
from admin import DB_FILE_PATH

logger = logging.getLogger(__name__)


class BaseModel(Model):
    database = SqliteDatabase(DB_FILE_PATH)

    class Meta:
        database = SqliteDatabase(DB_FILE_PATH)


class StatsRecord(BaseModel):
    id = PrimaryKeyField()
    schains_number = IntegerField(default=0)
    inserted_at = DateTimeField()

    tx_count_total = IntegerField(default=0)
    user_count_total = IntegerField(default=0)
    block_count_total = IntegerField(default=0)

    tx_count_24_hours = IntegerField(default=0)
    unique_tx_24_hours = IntegerField(default=0)
    user_count_24_hours = IntegerField(default=0)
    block_count_24_hours = IntegerField(default=0)
    gas_total_used_24_hours_gwei = FloatField(default=0)
    gas_total_used_24_hours_eth = FloatField(default=0)
    max_tps_last_24_hours = FloatField(default=0)

    tx_count_7_days = IntegerField(default=0)
    unique_tx_count_7_days = IntegerField(default=0)
    user_count_7_days = IntegerField(default=0)
    block_count_7_days = IntegerField(default=0)
    gas_total_used_7_days_gwei = FloatField(default=0)
    gas_total_used_7_days_eth = FloatField(default=0)
    max_tps_last_7_days = FloatField(default=0)

    tx_count_30_days = IntegerField(default=0)
    unique_tx_count_30_days = IntegerField(default=0)
    user_count_30_days = IntegerField(default=0)
    block_count_30_days = IntegerField(default=0)
    gas_total_used_30_days_gwei = FloatField(default=0)
    gas_total_used_30_days_eth = FloatField(default=0)
    max_tps_last_30_days = FloatField(default=0)

    @classmethod
    def add(cls, **kwargs):
        try:
            groups = None
            if kwargs.get('groups'):
                groups = kwargs.pop('groups')
            with cls.database.atomic():
                stats = cls.create(**kwargs)
                for group_stat in groups:
                    GroupStats.create(stats_record=stats, **group_stat)
            return stats, None
        except IntegrityError as err:
            logger.warning(err)
            return None, err

    @classmethod
    def get_last_stats(cls):
        try:
            group_by_days = []
            group_by_months = []
            raw_result = cls.select().order_by(cls.id.desc()).get()
            result = model_to_dict(raw_result, exclude=[cls.id])
            result['inserted_at'] = str(result['inserted_at'])
            for i in raw_result.group_stats:
                raw = model_to_dict(i, exclude=[GroupStats.stats_record, GroupStats.id])
                if raw['data_by_days']:
                    raw['tx_date'] = raw['tx_date'].strftime('%Y-%m-%d')
                    group_by_days.append(raw)
                else:
                    group_by_months.append(raw)
            result['group_by_days'] = group_by_days
            result['group_by_months'] = group_by_months
            return result
        except DoesNotExist:
            return None


class GroupStats(BaseModel):
    id = PrimaryKeyField()
    stats_record = ForeignKeyField(StatsRecord, related_name='group_stats')

    tx_count = IntegerField(default=0)
    unique_tx = IntegerField(default=0)
    user_count = IntegerField(default=0)
    gas_total_used_gwei = FloatField(default=0)
    gas_total_used_eth = FloatField(default=0)

    tx_date = DateField()
    data_by_days = BooleanField()


def create_tables():
    if not StatsRecord.table_exists():
        logger.info('Creating StatsRecord table...')
        StatsRecord.create_table()

    if not GroupStats.table_exists():
        logger.info('Creating GroupStats table...')
        GroupStats.create_table()
