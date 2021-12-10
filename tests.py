from freezegun import freeze_time
from datetime import datetime

from main import get_candle_periods_for_trades, get_previous_candle_periods


@freeze_time(datetime(2021, 12, 10, 11, 46, 3))
def test_get_candle_periods_for_rest():
    periods = list(get_previous_candle_periods())
    assert len(periods) == 1
    period = periods[0]
    assert period["resolution"] == 60
    assert period["start_time"].second == 0
    assert period["start_time"].minute == 45


def test_get_candle_periods_for_trades():
    time = datetime(2021, 12, 10, 11, 46, 3)
    periods = list(get_candle_periods_for_trades(time))
    assert len(periods) == 3
    for period in periods:
        if period["resolution"] == 60:
            assert period["start_time"].second == 0
            assert period["start_time"].minute == 46
            assert period["start_time"].hour == 11
            assert period["start_time"].day == 10
        if period["resolution"] == 3_600:
            assert period["start_time"].second == 0
            assert period["start_time"].minute == 0
            assert period["start_time"].hour == 11
            assert period["start_time"].day == 10
        if period["resolution"] == 24*3_600:
            assert period["start_time"].second == 0
            assert period["start_time"].minute == 0
            assert period["start_time"].hour == 0
            assert period["start_time"].day == 10
