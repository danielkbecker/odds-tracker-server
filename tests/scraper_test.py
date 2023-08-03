import datetime
from cloud_functions.scrape.websites import VegasInsiderScraper
import utils.helpers as helper_lib


# def test_scraper_cloud_function():
# #     # main.scraper_cloud_function('event', 'context')
# #
# #
# # def test_scraper_not_empty():
# #     # odds = main.VegasInsiderScraper("nba").scrape_odds()
# #     # futures = main.VegasInsiderScraper("nba").scrape_futures()
# #     # assert odds is not None
# #     # assert futures is not None


def test_rounding_down_timestamp_to_15min_interval_0():
    assert (helper_lib.round_down(0, 15) == 0)
    assert (helper_lib.round_down(1, 15) == 0)
    assert (helper_lib.round_down(15, 15) == 15)
    assert (helper_lib.round_down(16, 15) == 15)


def test_convert_odds_day_to_date_month_with_only_day_no_time_this_year():
    arg = VegasInsiderScraper("college-football")
    arg.timestamp = datetime.datetime(2022, 12, 1, 11, 30)
    assert helper_lib.convert_odds_day_to_date(arg, "Dec 02") == datetime.datetime(2022, 12, 2, 20, 30)


def test_convert_odds_day_to_date_month_with_only_day_no_time_next_year():
    arg = VegasInsiderScraper("college-football")
    arg.timestamp = datetime.datetime(2022, 12, 1, 11, 30)
    assert helper_lib.convert_odds_day_to_date(arg, "Jan 02") == datetime.datetime(2023, 1, 2, 20, 30)


def test_convert_odds_day_to_date_full_date_this_year():
    arg = VegasInsiderScraper("college-football")
    arg.timestamp = datetime.datetime(2022, 12, 1, 11, 30)
    assert helper_lib.convert_odds_day_to_date(arg, "Dec 16 11:30 AM ET") == datetime.datetime(2022, 12, 16, 11, 30)


def test_convert_odds_day_to_date_full_date_next_year():
    arg = VegasInsiderScraper("college-football")
    arg.timestamp = datetime.datetime(2022, 12, 1, 11, 30)
    assert helper_lib.convert_odds_day_to_date(arg, "Jan 16 11:30 AM ET") == datetime.datetime(2023, 1, 16, 11, 30)


def test_convert_odds_day_to_date():
    arg = VegasInsiderScraper("college-football")
    arg.timestamp = datetime.datetime(2022, 12, 1, 11, 30)
    assert helper_lib.convert_odds_day_to_date(arg, "Sunday 1:00 PM ET") != 'Foo'
