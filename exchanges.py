from ftx.rest.client import FtxClient as FtxRestClient
from ftx.websocket.client import FtxWebsocketClient
from datetime import datetime


class FtxRestClientExtended(FtxRestClient):
    def get_candles(self, market: str, resolution: int, start_time: float) -> dict:
        return self._get(
            f'/markets/{market}/candles?resolution={resolution}&start_time={start_time}'
        )


class FtxWebsocketClientExtended(FtxWebsocketClient):
    def __init__(self, queue, name):
        self.queue = queue
        self.name = name
        self.trade_count = 0
        super().__init__()

    def _handle_trades_message(self, message: dict) -> None:
        """receive the trade and put it in the queue"""
        self.trade_count += 1

        for data in message["data"]:
            time = datetime.strptime(data["time"], "%Y-%m-%dT%H:%M:%S.%f%z")
            # example: 2021-12-09T13:49:39.407690+00:00
            self.queue.put({
                "type":        "trade",
                "exchange":    self.name,
                "market":      message['market'],
                "liquidation": data["liquidation"],
                "price":       data["price"],
                "side":        data["side"],
                "size":        data["size"],
                "time":        time,
                "number":      self.trade_count,
            })


class Ftx:
    def __init__(self, markets, queue):
        self.markets = markets
        self.queue = queue
        self.name = "Ftx"

        self.websocket = FtxWebsocketClientExtended(queue, self.name)
        self.rest = FtxRestClientExtended()

    def subscribe_to_trades(self) -> None:
        """subscribe to markets"""
        for market in self.markets:
            self.websocket.get_trades(market)
            # todo: check how many market a single thread can support
            #         and create other websocket instances if needed

    def get_candle(self, resolution: int, start_time: float):
        """get candles for all markets for the given period and put them in the queue"""
        time_stamp = start_time.timestamp()
        for market in self.markets:
            candles = self.rest.get_candles(market, resolution, time_stamp)
            # yeah, sometimes more than 1

            count = 0
            for candle in candles:
                candle["time"] = candle["time"] / 1000  # comes in milliseconds
                if candle["time"] == time_stamp:
                    candle["market"] = market
                    candle["resolution"] = resolution
                    candle["exchange"] = self.name
                    candle["type"] = "candle"
                    self.queue.put(candle)
                    count += 1

            if count != 1:
                print("\n", "*" * 100, "candle pulled")
                print("EXCEPTION: server delivered other than just 1 candle")
                print(f"candles {candles}")
                print(f"start_time {start_time}")
                print(f"resolution {resolution}")
                print("*" * 100, "candle pulled")
