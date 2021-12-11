import os

from sqlalchemy import create_engine, ForeignKey, UniqueConstraint, DateTime
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Boolean, Float

Base = declarative_base()

SQL_FILE_NAME = os.getenv("SQL_FILE_NAME")
engine = create_engine(f'sqlite:///{SQL_FILE_NAME}', connect_args={'check_same_thread': False})


# todo: convert start_time, time fields below to DateTime column after switching to a
#  full fledged db. SqLite seems to not handle well:
#  https://docs.sqlalchemy.org/en/14/core/type_basics.html#sqlalchemy.types.DateTime

class Exchange(Base):
    # params: market_name, start_time, end_time

    __tablename__ = "exchange"

    name = Column(String, primary_key=True)
    trades = relationship("Trade", back_populates="exchange")
    candles = relationship("Candle", back_populates="exchange")


class Trade(Base):
    # params: market_name, start_time, end_time

    __tablename__ = "trade"

    id = Column(Integer, primary_key=True)
    exchange_name = Column(String, ForeignKey("exchange.name"))
    exchange = relationship("Exchange", back_populates="trades")
    market = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    side = Column(String, nullable=False)
    size = Column(Float, nullable=False)
    time = Column(Float, nullable=False)  # Unix time in seconds
    liquidation = Column(Boolean, nullable=False)

    def __str__(self):
        return f"Trade (id={self.id}, exchange_name={self.exchange_name}, market={self.market}," \
               f" price={self.price}, time={self.time}, side={self.side}, size={self.size}, )"


class Candle(Base):  # historical prices
    # params: market_name, start_time, end_time, # params: market_name, start_time, end_time

    __tablename__ = "candle"

    id = Column(Integer, primary_key=True)
    exchange_name = Column(String, ForeignKey("exchange.name"))
    exchange = relationship("Exchange", back_populates="candles")
    market = Column(String, nullable=False)
    resolution = Column(Integer, nullable=False)  # window length in seconds.
    # in secs: 15, 60*, 300, 900, 3600*, 14400, 86400*, or any multiple of 86400 up to 30*86400
    start_time = Column(Float, nullable=False)  # start time of the window/Unix time in seconds
    open = Column(Float, nullable=False)  # mark price at start_time
    close = Column(Float, nullable=False)  # mark price at the end of the window: start_time + resl.
    high = Column(Float, default=float('-inf'), nullable=False)  # highest price over the window
    low = Column(Float, default=float('+inf'), nullable=False)  # lowest price over the window
    volume = Column(Float, default=0, nullable=False)  # volume traded in the window

    def __str__(self):
        return f"Candle (id={self.id}, exchange_name={self.exchange_name}, market={self.market}," \
               f" open={self.open}, close={self.close}, high={self.high}, low={self.low}," \
               f" volume={self.volume}," \
               f" resolution={self.resolution}, start_time={self.start_time}, )"

    __table_args__ = (
        UniqueConstraint(
            'exchange_name', 'market', 'resolution', 'start_time',
            name='exch_market_resl_start_time_uc'),
    )


def get_or_create(session, model, commit=False, update=None, **kwargs):
    """return instance, created"""
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        if update:
            for key, value in update.items():
                setattr(instance, key, value)
        return instance, False
    else:
        update = update if update else {}
        instance = model(**kwargs, **update)
        session.add(instance)
        if commit:
            session.commit()
        return instance, True


if __name__ == "__main__":
    try:
        os.remove(SQL_FILE_NAME)
        print(f"Removed file {SQL_FILE_NAME}.")
    except OSError:
        pass  # file did not exist

    print(f"Created db: {SQL_FILE_NAME}.")
    Base.metadata.create_all(engine)
