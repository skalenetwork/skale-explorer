import os

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Float
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import create_engine
from sqlalchemy_utils.functions import database_exists

from admin import SERVER_DATA_DIR

Base = declarative_base()


class Stats(Base):
    __tablename__ = 'stats'

    id = Column(Integer, primary_key=True)
    schains_number = Column(Integer, nullable=False)
    inserted_at = Column(Integer, nullable=False)

    tx_count_total = Column(Integer, nullable=False)
    user_count_total = Column(Integer, nullable=False)

    tx_count_24_hours = Column(Integer, nullable=False)
    unique_tx_24_hours = Column(Integer, nullable=False)
    user_count_24_hours = Column(Integer, nullable=False)
    block_count_24_hours = Column(Integer, nullable=False)
    gas_total_used_24_hours_gwei = Column(Float, nullable=False)
    gas_total_used_24_hours_eth = Column(Float, nullable=False)
    max_tps_last_24_hours = Column(Float, nullable=False)

    tx_count_7_days = Column(Integer, nullable=False)
    unique_tx_count_7_days = Column(Integer, nullable=False)
    user_count_7_days = Column(Integer, nullable=False)
    block_count_7_days = Column(Integer, nullable=False)
    gas_total_used_7_days_gwei = Column(Float, nullable=False)
    gas_total_used_7_days_eth = Column(Float, nullable=False)
    max_tps_last_7_days = Column(Float, nullable=False)

    tx_count_30_days = Column(Integer, nullable=False)
    unique_tx_count_30_days = Column(Integer, nullable=False)
    user_count_30_days = Column(Integer, nullable=False)
    block_count_30_days = Column(Integer, nullable=False)
    gas_total_used_30_days_gwei = Column(Float, nullable=False)
    gas_total_used_30_days_eth = Column(Float, nullable=False)
    max_tps_last_30_days = Column(Float, nullable=False)

    def __repr__(self):
        return f'<Stats {self.tx_count_7_days}>'


class StatsDatabase:
    def __init__(self):
        self.db_path = f'sqlite:///{os.path.join(SERVER_DATA_DIR, "database.db")}'
        self.engine = create_engine(self.db_path, echo=True, future=True)
        if not database_exists(self.db_path):
            Base.metadata.create_all(self.engine)

    def add(self, **kwargs):
        with Session(self.engine) as session:
            with session.begin():
                session.add(Stats(**kwargs))
                session.commit()
