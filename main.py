import os
import sys
import queue
import threading
import time as time_module

from threading import Thread
from datetime import datetime, timedelta, timezone
from typing import Generator, Tuple

from sqlalchemy.orm import sessionmaker

from db import engine, get_or_create
from db import Exchange, Trade, Candle

import exchanges

# globals
COMMIT_EVERY_N_OBJECT = int(os.getenv("COMMIT_EVERY_N_OBJECT"))
DELAY_SECONDS_FROM_MINUTE = int(os.getenv("DELAY_SECONDS_FROM_MINUTE"))
ALERT_IF_Q_SIZE_MORE_THAN = int(os.getenv("ALERT_IF_Q_SIZE_MORE_THAN"))

# todo: make these 2 local for better testability
exchange_list = []
job_queue = queue.Queue()  # thread safe

Session = sessionmaker(bind=engine)
session = Session()


# TODO:
#  - more unit tests
#  - load tests


def parse_input(input_str: str) -> Generator[Tuple[str, list], None, None]:
    input_str = input_str.replace(" ", "")
    exchange_groups = input_str.split(";")
    for group in exchange_groups:
        exchange_name, markets = group.split(":")
        exchange_name = exchange_name.title()
        markets = markets.split(",")
        yield exchange_name, markets


def parse_input_and_subscribe_to_markets(input_str: str) -> None:
    for exchange_name, markets in parse_input(input_str):
        try:
            exchange_cls = getattr(exchanges, exchange_name)
        except AttributeError as error:
            print(f"Exchange {exchange_name} is not implemented!")
            raise error

        get_or_create(session, Exchange, name=exchange_name, commit=True)  # add to db
        exchange_obj = exchange_cls(markets, job_queue)
        exchange_list.append(exchange_obj)
        print(f"Exchange: {exchange_name}. Markets: {markets}.")


item_count = 0


def save_trade_and_update_candle(trade_dict: dict) -> None:
    """
    save trades to the db and update all 3 candles: min, hour, day
    the candle updated here is the one obtained via REST for the very first period
    subsequent candles are started from the first trade received via WebSocket
    """
    trade, _ = get_or_create(
        session, Trade,
        exchange_name=trade_dict["exchange"],
        market=trade_dict["market"],
        liquidation=trade_dict["liquidation"],
        price=trade_dict["price"],
        side=trade_dict["side"],
        size=trade_dict["size"],
        time=trade_dict["time"].timestamp(),
    )

    # update candles
    for period in get_current_candle_periods(trade_dict["time"]):
        candle, created = get_or_create(
            session, Candle,
            exchange_name=trade.exchange_name,
            market=trade.market,
            start_time=period["start_time"].timestamp(),
            resolution=period["resolution"],
        )
        candle.close = trade.price  # last trade will be effective
        # ^ assignment needs to be before session.commit()
        if created:  # start a candle
            print("\na trade started a candle")
            candle.open = trade.price
            session.commit()  # to get the default values which are at the SQL side
        candle.volume += trade.size
        candle.low = min(candle.low, trade.price)
        candle.high = max(candle.high, trade.price)


def save_candle_received_and_compare_with_calculated(received: dict) -> None:
    """ saves candle received from the REST API and compares to the one calculated from trades"""
    calculated = session.query(Candle).filter_by(
        exchange_name=received["exchange"],
        market=received["market"],
        start_time=received["time"],
        resolution=received["resolution"],
    ).first()
    if calculated is None:
        print("No trades prior to the received candles, nothing to compare!!")
        get_or_create(
            session, Candle,
            exchange_name=received["exchange"],
            market=received["market"],
            start_time=received["time"],
            resolution=received["resolution"],
            open=received["open"],
            close=received["close"],
            low=received["low"],
            high=received["high"],
            volume=received["volume"],
        )
    else:
        print("\n" + f"comparing candles received to calculated "
                     f"(resolution {calculated.resolution})".rjust(120, "_"))
        for attr in ["open", "close", "high", "low", "volume", ]:
            left = getattr(calculated, attr)
            right = received[attr]
            if not left == right:
                # print a report
                abs_diff = round(abs(left - right), 3)
                left_vs_right = f"{round(left, 3)} vs {round(right, 3)}"
                percent_diff = round(abs(left - right) / right * 100, 3)
                print(f"DISCREPANCY FOUND FOR {attr.ljust(7)}: diff:"
                      f" {str(abs_diff).ljust(15)}"
                      f", {str(left_vs_right).ljust(25)}"
                      f", {str(percent_diff).ljust(8)} %")
                # now update
                setattr(calculated, attr, right)
        print("\n".ljust(120, "_"))  # separator
        # tofix: there is an issue with the volume, it is probably sent cumulatively for the day
        #        (transferring from the previous candle would fix it)


def process_queue_item(item) -> None:
    """processes each queue item; saves objects to db for every COMMIT_EVERY_N_OBJECT of them"""
    global item_count
    item_count += 1

    # alert user if queue is too long
    q_size = job_queue.qsize()
    if q_size > ALERT_IF_Q_SIZE_MORE_THAN:
        print(f"\nQueue size: {q_size} {'Q' * (q_size // 10)}")

    if item["type"] == "candle":
        save_candle_received_and_compare_with_calculated(item)
    elif item["type"] == "trade":
        save_trade_and_update_candle(item)
        print("t", end="", flush=True)

    if item_count % COMMIT_EVERY_N_OBJECT == 0:
        print(" - committing objects to db..")
        session.commit()


def process_queue() -> None:
    """processes the queue"""
    while True:  # run forever
        try:
            item = job_queue.get()
            process_queue_item(item)
        except queue.Empty:
            print("queue empty..")
            time_module.sleep(0.1)


def get_current_candle_periods(time: datetime) -> Generator[dict, None, None]:
    """
    primarily for determining the periods of the trades coming in via websocket
    but is used for pulling candles via REST for its very initial pull
    when trades coming in, this will tell us which local candles to update
    brings the start time of min, hour, day that we are currently in, not over.
    will always return all three of min, hour, day
    """

    time = time.replace(second=0, microsecond=0)  # 11:46:03 => 11:46:00
    yield {"start_time": time, "resolution": 60}

    time = time.replace(minute=0)  # 11:46:03 => 11:00:00
    yield {"start_time": time, "resolution": 3_600}

    time = time.replace(hour=0)  # 11:46:03 => 00:00:00
    yield {"start_time": time, "resolution": 86_400}


def get_turned_candle_periods(time) -> Generator[dict, None, None]:  # i.e. min, hour, day
    """
    used to pull candles from the REST API (except the very first pull, see ^)
    brings the start time of min, hour, day that has just turned; see -1's below
    """
    print(f"{'Now: '.ljust(50)}: {time}")

    time = time.replace(second=0, microsecond=0)  # 11:46:03 => 11:46:00

    min_start_time = time + timedelta(minutes=-1)  # 11:46:03 => 11:45:00
    print(f"{'Getting the minute candle starting at: '.ljust(50)}: {min_start_time}")

    # minute job; always deliver bc this is a minute-ly job
    yield {"start_time": min_start_time, "resolution": 60}

    # hour job
    if time.minute == 0:
        hr_start_time = time + timedelta(hours=-1)  # 11:00:03 => 10:00:00
        print(f"{'Getting the hour candle starting at: '.ljust(50)}: {hr_start_time}")
        yield {"start_time": hr_start_time, "resolution": 3_600}

        # day job
        if time.hour == 0:
            day_start_time = time + timedelta(days=-1)  # 00:00:03 => 00:00:00 prev day
            print(f"{'Getting the day candle starting at: '.ljust(50)}: {day_start_time}")
            yield {"start_time": day_start_time, "resolution": 86_400}


def get_candles(first_time: bool = False) -> None:
    """
    gets the candles, by means of exchanges, and puts them in the job queue that we also put trades
    this is for them to be processed after all the trades received before them
    """
    # schedule next run first
    now = datetime.now(timezone.utc)
    if first_time:
        # wait until the minute to be over + few secs offset
        delay = 60 - now.second + DELAY_SECONDS_FROM_MINUTE  # in secs
        print(f"now: {now}")
        print(f"next candles will be get after {delay} seconds "
              f"({DELAY_SECONDS_FROM_MINUTE} secs offset)")
    else:
        delay = 60  # a min

    delay_get_candles(delay)  # schedule next run

    periods = get_current_candle_periods(now) if first_time else get_turned_candle_periods(now)
    # i.e. min, hour, day

    print("\n" + "Getting candles via REST".rjust(120, "_"))
    for period in periods:
        for exch in exchange_list:
            exch.get_candle(
                start_time=period["start_time"],
                resolution=period["resolution"],
            )


def delay_get_candles(delay: int = None) -> None:
    """ candles will be received at the turn of each period: min, hr, day + few secs of offset """
    threading.Timer(delay, get_candles).start()


if __name__ == "__main__":
    parse_input_and_subscribe_to_markets(sys.argv[1])
    # input format: "FTX:BTC-PERP,ETH-PERP; ROBIN:ABC-PERP"

    # process the jobs in the queue
    # the queue will have jobs both from the REST API and the WebSocket
    # could be replaced with ThreadPoolExecutor if 1 Thread is not enough
    Thread(target=process_queue).start()

    get_candles(first_time=True)
    # will reschedule itself to the turn of the minute first time, and a min thereafter

    for exchange in exchange_list:
        exchange.subscribe_to_trades()  # after getting the initial trades
