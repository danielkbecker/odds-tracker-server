import datetime

import pytest
import os
import main


def add_one_function(x):
    return x + 1


def test_add_one():
    assert add_one_function(3) == 4


def test_convert_odds_day_to_date_month_with_only_day_no_time_this_year():
    arg = main.VegasInsiderScraper("college-football")
    arg.now_timestamp = datetime.datetime(2022, 12, 1, 11, 30)
    assert main.VegasInsiderScraper.convert_odds_day_to_date(arg, "Dec 02") == datetime.datetime(2022, 12, 2, 20, 30)


def test_convert_odds_day_to_date_month_with_only_day_no_time_next_year():
    arg = main.VegasInsiderScraper("college-football")
    arg.now_timestamp = datetime.datetime(2022, 12, 1, 11, 30)
    assert main.VegasInsiderScraper.convert_odds_day_to_date(arg, "Jan 02") == datetime.datetime(2023, 1, 2, 20, 30)


def test_convert_odds_day_to_date_full_date_this_year():
    arg = main.VegasInsiderScraper("college-football")
    arg.now_timestamp = datetime.datetime(2022, 12, 1, 11, 30)
    assert main.VegasInsiderScraper.convert_odds_day_to_date(arg, "Dec 16 11:30 AM ET") == datetime.datetime(2022, 12,
                                                                                                             16, 11, 30)


def test_convert_odds_day_to_date_full_date_next_year():
    arg = main.VegasInsiderScraper("college-football")
    arg.now_timestamp = datetime.datetime(2022, 12, 1, 11, 30)
    assert main.VegasInsiderScraper.convert_odds_day_to_date(arg, "Jan 16 11:30 AM ET") == datetime.datetime(2023, 1,
                                                                                                             16, 11, 30)


def test_convert_odds_day_to_date():
    arg = main.VegasInsiderScraper("college-football")
    arg.now_timestamp = datetime.datetime(2022, 12, 1, 11, 30)
    assert main.VegasInsiderScraper.convert_odds_day_to_date(arg, "Sunday 1:00 PM ET") != 'Foo'