from datetime import datetime

from main import get_turned_candle_periods, get_current_candle_periods


def test_get_turned_candle_periods():
    time = datetime(2021, 12, 10, 11, 46, 3)
    periods = list(get_turned_candle_periods(time))
    assert len(periods) == 1
    period = periods[0]
    assert period["resolution"] == 60
    assert period["start_time"].second == 0
    assert period["start_time"].minute == 45


def test_get_current_candle_periods():
    time = datetime(2021, 12, 10, 11, 46, 3)
    periods = list(get_current_candle_periods(time))
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
        if period["resolution"] == 24 * 3_600:
            assert period["start_time"].second == 0
            assert period["start_time"].minute == 0
            assert period["start_time"].hour == 0
            assert period["start_time"].day == 10
