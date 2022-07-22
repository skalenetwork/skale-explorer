import logging
from playhouse.shortcuts import model_to_dict
from peewee import (Model, SqliteDatabase, IntegerField, DateTimeField,
                    FloatField, PrimaryKeyField, IntegrityError, DoesNotExist)
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
            with cls.database.atomic():
                stats = cls.create(**kwargs)
            return stats, None
        except IntegrityError as err:
            return None, err

    @classmethod
    def get_last_stats(cls):
        try:
            raw_result = cls.select().order_by(cls.id.desc()).get()
            result = model_to_dict(raw_result, exclude=[cls.id])
            result['inserted_at'] = str(result['inserted_at'])
            return result
        except DoesNotExist:
            return None


def create_tables():
    if not StatsRecord.table_exists():
        logger.info('Creating statsrecord table...')
        StatsRecord.create_table()
