from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime
import time
# import numpy as np
import os
from pytz import timezone
# import time
import helpers.helper_lib as helper_lib

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
VEGAS_INSIDER_SCHEMA = {
    "base_url": "https://vegasinsider.com/",
    "table_types": {
        "odds": {
            "bookmakers": ['open', 'consensus', 'fanduel', 'playmgm', 'caesars_sportsbook', 'wynnbet', 'betrivers',
                           'pointsbet',
                           'sports_illustrated', 'unibet'],
            "source_url": "/odds/las-vegas",
            "odds_css_classes": "d-flex flex-row hide-scrollbar odds-slider-all syncscroll tracks",
            "matchup_info_css_classes": "d-flex events flex-column position-sticky track",
            "odds_row_classes": "d-flex flex-row pr-2 pr-lg-0 px-1",
            "odds_box_classes": "odds-box"
        },
        "futures": {
            "css": {
                "grid": "d-flex flex-row hide-scrollbar odds-slider-all overflow-auto tracks",
                "teams": "d-flex flex-column position-relative",
                "odds": "d-flex flex-column",
            },
            "source_url": "/odds/",
            "bookmakers": ['fanduel', 'playmgm', 'caesars_sportsbook', 'wynnbet', 'betrivers', 'pointsbet',
                           'sports_illustrated', 'unibet'],
            "future_types": {
                "nfl": {
                    "team": {
                        "types": ["super_bowl", "afc_champ", "nfc_champ", "afc_east", "afc_south", "afc_north",
                                  "afc_west", "nfc_east", "nfc_south", "nfc_north", "nfc_west"],
                        "urls": ["futures", "afc-championship", "nfc-championship", "afc-east", "afc-south",
                                 "afc-north", "afc-west", "nfc-east", "nfc-south", "nfc-north", "nfc-west"]
                    },
                    "player": {
                        "types": ["mvp", "rookie", "passing_yards", "receiving_yards", "rushing_yards",
                                  "passing_touchdowns", "receiving_touchdowns"],
                        "urls": ["mvp", "rookie-of-the-year", "most-passing-yards", "most-receiving-yards",
                                 "most-rushing-yards", "most-passing-touchdowns", "most-receiving-touchdowns"]
                    }
                },
                "nba": {
                    "team": {
                        # Careful about picking the first grid on each page
                        "types": ["nba_champ", "eastern_conference", "western_conference", "atlantic_division",
                                  "central_division", "northwest_division", "pacific_division", "southeast_division",
                                  "southwest_division"],
                        "urls": ["futures", "eastern-conference", "western-conference", "atlantic-division",
                                 "central-division", "northwest-division", "pacific-division", "southeast-division",
                                 "southwest-division"]
                    },
                    "player": {
                        "types": ["mvp", "roy", "dpoy", "mip", "sixth_man"],
                        "urls": ["mvp", "rookie-of-the-year", "defensive-player-of-the-year", "most-improved",
                                 "sixth-man"]
                    }
                },
                "nhl": {
                    "team": {
                        "types": ["stanley_cup", "eastern_conference", "western_conference", "atlantic_divsion",
                                  "metropolitan_division", "pacific_division", "central_division"],
                        "urls": ["futures", "eastern-conference", "western-conference", "atlantic-division",
                                 "metropolitan-division", "pacific-division", "central-division"]
                    },
                    "player": {
                        # NHL needs its own thing because it's not in a table
                        "types": ["hart_award", "vezina_award", "rocket_richard", "calder", "james_norries",
                                  "jack_adams"],
                        "urls": ["", "", "", "", "", ""]
                    }
                },
                "college-football": {
                    "team": {
                        "types": [],
                        "urls": []
                    },
                    "player": {
                        "types": [],
                        "urls": []
                    }
                }
            }
        },
        "schedule": {
            "source_url": "/schedule"
        },
    },
    "sports": {
        "nfl": {
            "sport_key": "nfl",
            "sport_label": "NFL",
            "bet_order": ["line", "overunder", "moneyline"]
        },
        "nba": {
            "sport_key": "nba",
            "sport_label": "NBA",
            "bet_order": ["line", "overunder", "moneyline"]
        },
        "nhl": {
            "sport_key": "nhl",
            "sport_label": "NHL",
            "bet_order": ["moneyline", "overunder", "puckline"]
        },
        "college-football": {
            "sport_key": "college-football",
            "sport_label": "NCAAF",
            "bet_order": ["line", "overunder", "moneyline"]
        }
    }
}


class VegasInsiderScraper:
    def __init__(self, sport):
        self.sport = sport
        self.headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
        }
        self.parser = "html.parser"
        self._timestamp = datetime.datetime.now(TIMEZONE)

    @staticmethod
    def convert_odds_day_to_date(ts, day):
        if day == 'Live': # or
            date = None
        elif 'Today' in day:
            date = datetime.datetime.combine(ts.date(),
                                             datetime.datetime.strptime(day.split(' ')[1] + day.split(' ')[2],
                                                                        "%I:%M%p").time())
        elif 'Tomorrow' in day:
            date = datetime.datetime.combine(ts.date() + datetime.timedelta(days=1),
                                             datetime.datetime.strptime(day.split(' ')[1] + day.split(' ')[2],
                                                                        "%I:%M%p").time())
        # Is a month
        elif day.split(' ')[0] in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Aug', 'Nov', 'Dec']:
            month_of_game = datetime.datetime.strptime(day.split(' ')[0], "%b").month
            month_of_today = ts.month
            # Game is this year
            if month_of_game >= month_of_today:
                year_of_game = ts.year
            else:
                year_of_game = ts.year + 1

            if len(day.split(' ')) == 2:
                month_and_day = datetime.datetime.strptime(day, "%b %d")
                date = datetime.datetime(year_of_game, month_and_day.month, month_and_day.day, 20, 30)
            else:
                date_without_tz = day[slice(0, -3)] + " " + str(year_of_game)
                date = datetime.datetime.strptime(date_without_tz, "%b %d %I:%M %p %Y")
        else:
            # Monday, Tuesday, Wednesday, etc...
            today_day_of_wk = ts.isoweekday()
            # For some reason it thinks %A is nov ?
            game_day_of_wk = time.strptime(day.split(" ")[0], "%A").tm_wday + 1
            game_time = datetime.datetime.strptime(day.split(' ')[1] + day.split(' ')[2], "%I:%M%p").time()
            if today_day_of_wk < game_day_of_wk:
                days_ahead = game_day_of_wk - today_day_of_wk
            else:
                days_ahead = 7 - ts.date().isoweekday() + game_day_of_wk
            day_of_game = ts.date() + datetime.timedelta(days=days_ahead)
            date = datetime.datetime.combine(day_of_game, game_time)
        return date

    # ****** TRANSFORMATION FUNCTIONS START ******
    def transform_odds_list_to_dataframe_row(self, game_matchup, header):
        df = pd.DataFrame()
        df["game_date"] = [self.convert_odds_day_to_date(self._timestamp, game_matchup[0])]
        df["away_team_full_name"] = [game_matchup[1]]
        df["home_team_full_name"] = [game_matchup[2]]
        df["away_team_code"] = [game_matchup[3]]
        df["home_team_code"] = [game_matchup[4]]
        df["header_or_week"] = [header]
        df["update_timestamp"] = self._timestamp
        bet_types = VEGAS_INSIDER_SCHEMA["sports"][self.sport]["bet_order"]
        away_home_order = ["away", "home"]

        col_names = []
        col_index = 5
        temp_array = []

        for bet_type_index, bet_type in enumerate(bet_types, start=1):
            for home_away_index, home_or_away in enumerate(away_home_order, start=1):
                for bookmaker_index, bookmaker in enumerate(VEGAS_INSIDER_SCHEMA["table_types"]["odds"]["bookmakers"],
                                                            start=1):
                    odd_col_name = bet_type + '_' + home_or_away + '_' + bookmaker + '_' + "odds"
                    cost_col_name = bet_type + '_' + home_or_away + '_' + bookmaker + '_' + "cost"
                    # If it is a moneyline bet there is only 1 value in the cell.
                    if bet_type == 'moneyline':
                        col_names.append(cost_col_name)
                        temp_array.append(game_matchup[col_index])
                        col_index += 1
                    else:
                        if game_matchup[col_index] == 'N/A':
                            col_names.append(odd_col_name)
                            col_names.append(cost_col_name)
                            temp_array.append(game_matchup[col_index])
                            temp_array.append(game_matchup[col_index])
                            col_index += 1
                        else:
                            col_names.append(odd_col_name)
                            col_names.append(cost_col_name)
                            temp_array.append(game_matchup[col_index])
                            col_index += 1
                            temp_array.append(game_matchup[col_index])
                            col_index += 1
        df_dict = dict(zip(col_names, temp_array))
        df_new_row = pd.DataFrame(df_dict, index=[0])
        df = pd.concat([df, df_new_row], axis=1)
        df = helper_lib.sha_256_hash(df, col_names, name="hash")
        return df

    # Inputs: None, this method inherits a sport from its parent class.
    # Outputs: Pandas Dataframe with odds
    def scrape_odds(self):
        html = BeautifulSoup(requests.get(
            VEGAS_INSIDER_SCHEMA["base_url"] + self.sport + VEGAS_INSIDER_SCHEMA["table_types"]["odds"]["source_url"],
            self.headers).content, self.parser)
        css_search = VEGAS_INSIDER_SCHEMA["table_types"]["odds"]["odds_css_classes"]
        odds_html = html.find_all("div", class_=css_search)
        header_text = html.find("h1").text
        odds_array = []
        for matchup_index, each_matchup in enumerate(odds_html):
            matchup_info = each_matchup.find("div", class_=VEGAS_INSIDER_SCHEMA["table_types"]["odds"][
                "matchup_info_css_classes"]).getText(",", strip=True).replace(',Bet Now', '')
            matchup_array = [matchup_info]
            matchup_rows = each_matchup.find_all("div",
                                                 class_=VEGAS_INSIDER_SCHEMA["table_types"]["odds"]["odds_row_classes"])
            for matchup_row in matchup_rows:
                odd_boxes = matchup_row.find_all("div",
                                                 class_=VEGAS_INSIDER_SCHEMA["table_types"]["odds"]["odds_box_classes"])
                for odds_box in odd_boxes:
                    in_box = odds_box.getText(",", strip=True).replace(',Bet Now', '')
                    if len(in_box) == 0:
                        matchup_array.append("N/A")
                    else:
                        matchup_array.append(in_box)
            matchup_array = ",".join(matchup_array)
            odds_array.append(matchup_array)
        " ".join(odds_array)
        odds_df_array = []
        for matchup in odds_array:
            split_matchup = matchup.split(',')
            if split_matchup[0] != 'Final' and split_matchup[0] != 'Canceled':
                temp_df = self.transform_odds_list_to_dataframe_row(split_matchup, header_text)
                odds_df_array.append(temp_df)
        if len(odds_df_array) > 0:
            odds_dataframe = pd.concat(odds_df_array, ignore_index=True)
        else:
            odds_dataframe = pd.DataFrame()
        return odds_dataframe

    def scrape_team_futures(self):
        # Following line should go through
        team_futures_df = pd.DataFrame()
        team_futures = VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["future_types"][self.sport]["team"]
        for i, odds_type in enumerate(team_futures["urls"]):
            html = BeautifulSoup(requests.get(
                VEGAS_INSIDER_SCHEMA["base_url"] + self.sport + '/odds/' + odds_type,
                self.headers).content, self.parser)
            css_grid_search = VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["css"]["grid"]
            css_team_search = VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["css"]["teams"]
            # css_odds_search = VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["css"]["odds"]
            grid_html = html.find("div", class_=css_grid_search)
            if grid_html is not None:
                teams = grid_html.find("div", class_=css_team_search).getText(",", strip=True).split(',')[::2]
                # odds_html = grid_html.find_all("div", class_=css_odds_search)
                futures_in_grid_array = []
                future_cols = grid_html.find_all("div", class_="d-flex flex-column")
                temp_team_odds = pd.DataFrame()
                for future_col in future_cols:
                    odds_boxes = future_col.find_all("div", class_=["m-1 odds-box", "m-1 odds-box position-relative",
                                                                    "best-odds-box m-1 odds-box position-relative",
                                                                    "best-odds-box m-1 odds-box"])
                    odds_col_list = []
                    for odds_box in odds_boxes:
                        in_box = odds_box.getText(",", strip=True).replace(',Bet Now', '')
                        if len(in_box) == 0:
                            odds_col_list.append("N/A")
                        else:
                            odds_col_list.append(in_box)
                    futures_in_grid_array.append(odds_col_list)
                temp_team_odds["team"] = teams
                temp_team_odds["future_type"] = team_futures["types"][i]
                new_df = pd.DataFrame(zip(*futures_in_grid_array))
                temp_team_odds = pd.concat([temp_team_odds, new_df], axis=1, ignore_index=True)
                team_futures_df = pd.concat([team_futures_df, temp_team_odds], axis=0, ignore_index=True)
        col_names_to_hash = ["team_name", "future_type"] + VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["bookmakers"]
        if len(team_futures["types"]) > 0:
            team_futures_df.columns = col_names_to_hash
            team_futures_df = helper_lib.sha_256_hash(team_futures_df, col_names_to_hash, name="hash")
            team_futures_df["update_timestamp"] = self._timestamp
        return team_futures_df

    # Need to do this next.
    def get_player_futures(self):
        # temp_player_odds = pd.DataFrame()
        # player_odds = VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["future_types"][self.sport]["player_odds"]
        pass

    @property
    def return_timestamp(self):
        return self._timestamp


# This is entry point for gcloud functions.
def scraper_cloud_function(event, context):
    # if(event["sport"] == "nba"):
    #     nba_scraper = VegasInsiderScraper(event["sport"])
    #     nba_scraper.scrape_all_tables()
    #     nba_scraper.upload_to
    print("Scraper job started.", event, context)
    total_rows_added = 0
    for sport in VEGAS_INSIDER_SCHEMA["sports"].keys():
        # These are the features of VegasInsider we are scraping. This could probably be abstracted.
        scrape_sport = VegasInsiderScraper(sport)
        current_lines = {
            "odds": scrape_sport.scrape_odds(),
            "team_futures": scrape_sport.scrape_team_futures(),
        }
        ts = scrape_sport.return_timestamp
        for (k, v) in current_lines.items():
            if not v.empty:
                temp_table_name = "dev_temp_" + sport.replace("-", "_") + '_' + k
                target_table_name = "dev_" + sport.replace("-", "_") + '_' + k
                # Returns a count of rows that it uploaded.
                rowcount = helper_lib.save_diff_to_mysql(mysql_username, mysql_password, mysql_host, mysql_port,
                                                         mysql_dbname, v, temp_table_name, target_table_name)
                total_rows_added += rowcount
                helper_lib.save_diff_to_s3(v, ts, sport, k, s3_access_key_id, s3_secret_access_key)
            else:
                print("Not scraping " + sport + " " + k.replace("_", " ") + ".")
    print("Scraper job finished. " + str(total_rows_added) + " total rows added.")


scraper_cloud_function('', '')
