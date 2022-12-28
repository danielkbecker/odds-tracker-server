# from bs4 import BeautifulSoup
# import requests
# import pandas as pd
import datetime
# import time
# import numpy as np
import os
from pytz import timezone
import helpers.helper_lib as helper_lib

# import time
# import helpers.helper_lib as helper_lib

# Import Environment Variables
mysql_username = os.environ["MYSQL_USERNAME"]
mysql_password = os.environ["MYSQL_PW"]
mysql_host = os.environ["MYSQL_HOST"]
mysql_dbname = os.environ["MYSQL_DB_NAME"]
mysql_port = os.environ["MYSQL_DB_PORT"]
s3_access_key_id = os.environ["S3_ACCESS_KEY_ID"]
s3_secret_access_key = os.environ["S3_SECRET_ACCESS_KEY"]
s3_bucket_endpoint = os.environ["S3_BUCKET_ENDPOINT"]

# Set Local Global Variables and Configurations
TIMEZONE = timezone('EST')
BUSINESS_LOGIC = {
    "base_url": "https://vegasinsider.com",
    "sports": {
        "nfl": {
            "sport_key": "nfl",
            "sport_label": "NFL",
            "bet_order": ["line", "overunder", "moneyline"],
            "table_types": {
                "team_futures": {
                    "subtypes": {
                        # Key - URL
                        "super_bowl": {
                            "url": "futures"
                        },
                        "afc_champ": {
                            "url": "afc-championship"
                        },
                        "nfc_champ": {
                            "url": "nfc-championship"
                        },
                        "afc_east": {
                            "url": "afc-east"
                        },
                        "afc_south": {
                            "url": "afc-south"
                        },
                        "afc_north": {
                            "url": "afc-north"
                        },
                        "afc_west": {
                            "url": "afc-west"
                        },
                        "nfc_east": {
                            "url": "nfc-east"
                        },
                        "nfc_south": {
                            "url": "nfc-south"
                        },
                        "nfc_north": {
                            "url": "nfc-north"
                        },
                        "nfc_west": {
                            "url": "nfc-west"
                        }
                    }
                },
                "player_futures": {
                    "subtypes": {},
                    # "types": [],
                    # "urls": ["mvp", "rookie-of-the-year", "most-passing-yards", "most-receiving-yards",
                    #          "most-rushing-yards", "most-passing-touchdowns", "most-receiving-touchdowns"]
                },
                "odds": {
                    "subtypes": {
                        "game_odds": {
                            "url": "odds/las-vegas"
                        }
                    },
                }
            },

        },
        "nba": {
            "sport_key": "nba",
            "sport_label": "NBA",
            "bet_order": ["line", "overunder", "moneyline"],
            "table_types": {
                "team_futures": {
                    "subtypes": {
                        "nba_champ": {
                            "url": "futures"
                        },
                        "eastern_conference": {
                            "url": "eastern-conference"
                        },
                        "western_conference": {
                            "url": "western-conference"
                        },
                        "atlantic_division": {
                            "url": "atlantic-division"
                        },
                        "central_division": {
                            "url": "central-division"
                        },
                        "northwest_division": {
                            "url": "northwest-division"
                        },
                        "pacific_division": {
                            "url": "pacific-division"
                        },
                        "southeast_division": {
                            "url": "southeast-division"
                        },
                        "southwest_division": {
                            "url": "southwest-division"
                        }
                    }
                },
                # "player_futures":  {
                # "subtypes": {

                # }
                # },
                "odds": {
                    "subtypes": {
                        "game_odds":
                            {
                                "url": "odds/las-vegas"
                            }
                    }
                }
            }
        },
        "nhl": {
            "sport_key": "nhl",
            "sport_label": "NHL",
            "bet_order": ["moneyline", "overunder", "puckline"],
            "table_types": {
                "team_futures": {
                    "subtypes": {
                        "stanley_cup": {
                            "url": "futures"
                        },
                        "eastern_conference": {
                            "url": "eastern-conference"
                        },
                        "western_conference":
                            {"url": "western-conference"},
                        "atlantic_divsion": {"url": "atlantic-division"},
                        "metropolitan_division": {"url": "metropolitan-division"},
                        "pacific_division": {"url": "pacific-division"},
                        "central_division": {"url": "central-division"}
                    },
                },
                "player_futures": {
                    "subtypes": {},
                },
                "odds": {
                    "subtypes": {
                        "game_odds":
                            {
                                "url": "odds/las-vegas"
                            }
                    }
                }
            }
        },
        "college-football": {
            "sport_key": "college-football",
            "sport_label": "NCAAF",
            "bet_order": ["line", "overunder", "moneyline"],
            "table_types": {
                # "team_futures": {
                # "subtypes": [],
                # "urls": []
                # },
                # "player_futures": {
                #    "subtypes": [],
                #    "urls": []
                # },
                "odds": {
                    "subtypes": {
                        "game_odds":
                            {
                                "url": "odds/las-vegas"
                            }
                    },
                }
            }
        }
    },
    "table_types": {
        # Hopefully I can import this from an external config file which maps to the table_types in
        # a general sports dictionary which spans several websites with similar schemas.
        "odds": {
            # "css": {
            #     "grid": "d-flex flex-row hide-scrollbar odds-slider-all overflow-auto tracks",
            #     "teams": "d-flex flex-column position-relative",
            #     "odds": "d-flex flex-column",
            # },
            #
            "source_url": "/odds/las-vegas",
            "missing_bookmakers": [],
            "odds_css_classes": "d-flex flex-row hide-scrollbar odds-slider-all syncscroll tracks",
            "matchup_info_css_classes": "d-flex events flex-column position-sticky track",
            "odds_row_classes": "d-flex flex-row pr-2 pr-lg-0 px-1",
            "odds_box_classes": "odds-box"
        },
        "team_futures": {
            "css": {
                "grid": "d-flex flex-row hide-scrollbar odds-slider-all overflow-auto tracks",
                "teams": "d-flex flex-column position-relative",
                "odds": "d-flex flex-column",
            },
            "source_url": "/odds/",
            "missing_bookmakers": ['open', 'consensus'],

        },
        "player_futures": {
            "css": {},
            "source_url": "/odds/",
            "missing_bookmakers": ['open', 'consensus'],
        }
        # "schedules": { }
    },
    "bookmakers":
        ['open', 'consensus', 'fanduel', 'playmgm', 'caesars_sportsbook', 'wynnbet', 'betrivers',
         'pointsbet',
         'sports_illustrated', 'unibet']
}


class Scraper:
    def __init__(self):
        # self.soup = self.get_soup()
        self.headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
        }
        self.parser = "html.parser"
        self._timestamp = datetime.datetime.now(TIMEZONE)

    def extract_data(self, sport, table_type, subtype):
        """
        Extract data from and return a dictionary of data.
        :param sport: str
        :param table_type: str
        :param subtype: str
        :return: dict
        """

    def transform_data(self, data, sport, table_type, subtype):
        """
        Transform data into a standardized format.
        :param data: dict
        :param sport: str
        :param table_type: str
        :param subtype: str
        :return: dict
        """

    def load_data(self, data, sport, table_type, subtype):
        """
        Load data into a database.
        :param data: dict
        :param sport: str
        :param table_type: str
        :param subtype: str
        :return: None
        """


class VegasInsiderScraper(Scraper):
    def __init__(self):
        super().__init__()
        # self.soup = self.get_soup()

    # def scrape_html(self, url):
    #     return str(url)
    # return BeautifulSoup(requests.get(url, headers=self.headers).content, self.parser)

    def extract_data(self, sport, table_type_values, bet_subtypes):
        for i, subtype in enumerate(bet_subtypes):
            url = f"https://www.vegasinsider.com/{sport}{table_type_values['urls'][i]}"
            print(url)
        data = "data"
        return data
        # print(url, self.scrape_html(url))

    def transform_data(self, data, **kwargs):
        pass

    def load_data(self, data, **kwargs):
        pass

    # soup = self.scrape_html(url)
    # print(soup)
    # if bet_type == "odds":
    #     return self.fetch_odds(bet_subtypes)
    # elif bet_type == "team_futures":
    #     return self.fetch_team_futures(bet_subtypes)
    # elif bet_type == "player_futures":
    #     return self.fetch_player_futures(bet_subtypes)
    # else:
    #     return None


# sports = nba, nfl, nhl, college-football
# bet_types = odds, team_futures, player_futures
# bet_types = stanley_cup, eastern_conference, mvp, rookie_of_the_year, etc.
def scrape_vegas_insider():
    for sport in BUSINESS_LOGIC["sports"].keys():
        for (k, v) in BUSINESS_LOGIC["table_types"].items():
            pass
            # subtypes = BUSINESS_LOGIC["sports"][sport]["table_types"][k]["subtypes"]
            # Does the sport have any subtypes to scrape?
            # if subtypes:
            # print(sport, v["source_url"], subtypes)
            # VegasInsiderScraper().fetch_data(sport, k, bet_subtypes["urls"])
            # print(sport, v, bet_subtypes)
            # scraped_table = VegasInsiderScraper(sport).fetch_data(v['source_url'], bet_subtypes)
            # helper_lib.save_diff_to_mysql(scraped_table, sport, bet_type, bet_subtypes["types"])
            # helper_lib.save_diff_to_s3(scraped_table, sport, bet_type, bet_subtypes["types"])
            #     print(f"https://www.vegasinsider.com/" + sport +
            #           BUSINESS_LOGIC["bet_types"][bet_type]["source_url"] + bet_url + "/")3


def scrape_routine():
    scrape_vegas_insider()


def scraper_cloud_function(event, context):
    print(event, context)
    print("Scraper Started")
    scrape_routine()


scraper_cloud_function(None, None)
