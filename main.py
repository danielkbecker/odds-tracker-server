from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime
import numpy as np
import os
import hashlib
from pytz import timezone
import time
from sqlalchemy import create_engine
import pymysql
import boto3
from io import StringIO

# ** Gcloud deploy command **


AWS_USERNAME = os.environ["AWS_MASTER_USERNAME"]
AWS_PASSWORD = os.environ["AWS_MASTER_PW"]
AWS_DB_ENDPOINT = os.environ["AWS_RDS_DB_ENDPOINT"]
AWS_RDS_DB = os.environ["AWS_RDS_DB"]
AWS_DB_PORT = os.environ["AWS_RDS_DB_PORT"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_S3_BUCKET_ENDPOINT = os.environ["AWS_S3_BUCKET_ENDPOINT"]

TIMEZONE = timezone('EST')
VEGAS_INSIDER_SCHEMA = {
    "base_url": "http://vegasinsider.com/",
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
                    "team_odds": {
                        "types": ["super_bowl", "afc_champ", "nfc_champ", "afc_east", "afc_south", "afc_north",
                                  "afc_west", "nfc_east", "nfc_south", "nfc_north", "nfc_west"],
                        "urls": ["futures", "afc-championship", "nfc-championship", "afc-east", "afc-south",
                                 "afc-north", "afc-west", "nfc-east", "nfc-south", "nfc-north", "nfc-west"]
                    },
                    "player_odds": {
                        "types": ["mvp", "rookie", "passing_yards", "receiving_yards", "rushing_yards",
                                  "passing_touchdowns", "receiving_touchdowns"],
                        "urls": ["mvp", "rookie-of-the-year", "most-passing-yards", "most-receiving-yards",
                                 "most-rushing-yards", "most-passing-touchdowns", "most-receiving-touchdowns"]
                    }
                },
                "nba": {
                    "team_odds": {
                        # Careful about picking the first grid on each page
                        "types": ["nba_champ", "eastern_conference", "western_conference", "atlantic_division",
                                  "central_division", "northwest_division", "pacific_division", "southeast_division",
                                  "southwest_division"],
                        "urls": ["futures", "eastern-conference", "western-conference", "atlantic-division",
                                 "central-division", "northwest-division", "pacific-division", "southeast-division",
                                 "southwest-division"]
                    },
                    "player_odds": {
                        "types": ["mvp", "roy", "dpoy", "mip", "sixth_man"],
                        "urls": ["mvp", "rookie-of-the-year", "defensive-player-of-the-year", "most-improved",
                                 "sixth-man"]
                    }
                },
                "nhl": {
                    "team_odds": {
                        "types": ["stanley_cup", "eastern_conference", "western_conference", "atlantic_divsion",
                                  "metropolitan_division", "pacific_division", "central_division"],
                        "urls": ["futures", "eastern-conference", "western-conference", "atlantic-division",
                                 "metropolitan-division", "pacific-division", "central-division"]
                    },
                    "player_odds": {
                        # NHL needs its own thing because its not in a table
                        "types": ["hart_award", "vezina_award", "rocket_richard", "calder", "james_norries",
                                  "jack_adams"],
                        "urls": ["", "", "", "", "", ""]
                    }
                },
                "college-football": {
                    "team_odds": {
                        "types": [],
                        "urls": []
                    },
                    "player_odds": {
                        "types": [],
                        "urls": []
                    }
                }
            }
        },
        "schedule": {
            "source_url": "/schedule"
        },
        "matchups": {
            "source_url": "/matchups"
        }
    },
    "sports": {
        "nfl": {
            "sport_key": "nfl",
            "sport_label": "NFL",
            "bet_order": ["line", "overunder", "moneyline"],
            "table_names": {
                "prod_odds": "nfl_odds",
                "prod_temp_odds": "temp_nfl_odds",
                "prod_team_futures": "nfl_team_futures",
                "prod_temp_team_futures": "temp_nfl_team_futures"
            }
        },
        "nba": {
            "sport_key": "nba",
            "sport_label": "NBA",
            "bet_order": ["line", "overunder", "moneyline"],
            "table_names": {
                "prod_odds": "nba_odds",
                "prod_temp_odds": "temp_nba_odds",
                "prod_team_futures": "nba_team_futures",
                "prod_temp_team_futures": "temp_nba_team_futures"
            }
        },
        "nhl": {
            "sport_key": "nhl",
            "sport_label": "NHL",
            "bet_order": ["moneyline", "overunder", "puckline"],
            "table_names": {
                "prod_odds": "nhl_odds",
                "prod_temp_odds": "temp_nhl_odds",
                "prod_team_futures": "nhl_team_futures",
                "prod_temp_team_futures": "temp_nhl_team_futures"
            }
        },
        "college-football": {
            "sport_key": "college-football",
            "sport_label": "NCAAF",
            "bet_order": ["line", "overunder", "moneyline"],
            "table_names": {
                "prod_odds": "college_football_odds",
                "prod_temp_odds": "temp_college_football_odds",
                "prod_team_futures": "college_football_team_futures",
                "prod_temp_team_futures": "temp_college_football_team_futures"
            }
        }
    }
}


# Imports
# Rounds the odds to the lowest hour/min where is a multiple of x. I'm using 15 min in this script.
def round_down(num, divisor):
    return num - (num % divisor)


# 'GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES,
# CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW,
# CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER ON *.* TO `admin`@`%` WITH GRANT OPTION'
# Should Import this from external file ****
def save_data(sport, dataframe, temp_table_name, table_name):
    db_url = "mysql+pymysql://" + AWS_USERNAME + ":" + AWS_PASSWORD + "@" + AWS_DB_ENDPOINT + ":" + AWS_DB_PORT + "/" + AWS_RDS_DB
    engine = create_engine(db_url)
    with engine.connect() as connection:
        if not dataframe.empty:
            try:
                frame = dataframe.to_sql(table_name, connection, index=False)
            except ValueError as vx:
                # Table exists
                temp_frame = dataframe.to_sql(temp_table_name, connection, if_exists='replace')
                # This will break if the table does not exist
                before_rows_query = "SELECT COUNT(*) FROM " + table_name
                before_rows = connection.execute(before_rows_query).fetchone()[0]
                sql_string = "INSERT INTO " + table_name + " (" + ", ".join(
                    dataframe.columns.tolist()) + ") " + "SELECT " + ", ".join(
                    dataframe.columns.tolist()) + " FROM " + temp_table_name + " WHERE (" + "hash" + ") NOT IN (SELECT " + "hash" + " FROM " + table_name + ")"
                connection.execute(sql_string)
                after_rows_query = "SELECT COUNT(*) FROM " + table_name
                after_rows = connection.execute(after_rows_query).fetchone()[0]
                print(vx)
                print(str(int(after_rows) - int(before_rows)) + ' new rows added to ' + table_name)
            except Exception as ex:
                print(ex)
            else:
                print(f"{table_name} created successfully.")
    print("Finished")


# Function saves a pandas dataframe to a s3 bucket.
def save_to_s3(dataframe, timestamp, sport, bet_type):
    bucket = 'oddstracker'
    # Need the date and time rounded down to the nearest 15 min interval here
    month = str(timestamp.month)
    day = str(timestamp.day)
    hour = str(timestamp.hour)
    minute = timestamp.minute
    # Not sure whether to store by bet_type or day first.
    filename = 'scraped_data' + '/' + sport + '/' + bet_type + '/' + month + '/' + day + '/' + hour + ':' \
               + str(round_down(minute, 15)) + '.csv'

    print(filename)
    csv_buffer = StringIO()
    odds_dataframe = dataframe.to_csv(csv_buffer, index=False)
    s3csv = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    response = s3csv.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=filename)


class VegasInsiderScraper:
    def __init__(self, sport):
        # Each class instance is for a particular sport.
        self.sport = sport
        # Spoofing ourselves as a browser
        self.headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
        }
        self.parser = "html.parser"
        self.now_timestamp = datetime.datetime.now(TIMEZONE)

    # ****** HELPER FUNCTIONS START ******
    # Input: Tomorrow 4:05 PM ET, Today 1:00PM ET, Monday 8:15PM ET
    # Output: Datetime datatype
    # Notes:
    # If day has Live then use now.
    # If day has Today, then use today's date and strip out the time
    # If day has tomorrow, then use today's date + 1 and strip out the time
    # If day has a day of the week, then get figure out how many days in front of today that day is and shift
    # current day that far. Then strip out the date. Need to use date.isoweekday for both values.
    # If now timestamp is a higher value than the other timestamp,
    # then that means the day is next week. So do 7 - now + that day
    def convert_odds_day_to_date(self, day):
        if day == 'Live':
            date = str(self.now_timestamp)
        elif 'Today' in day:
            date = datetime.datetime.combine(self.now_timestamp.date(),
                                             datetime.datetime.strptime(day.split(' ')[1] + day.split(' ')[2],
                                                                        "%I:%M%p").time())
        elif 'Tomorrow' in day:
            date = datetime.datetime.combine(self.now_timestamp.date() + datetime.timedelta(days=1),
                                             datetime.datetime.strptime(day.split(' ')[1] + day.split(' ')[2],
                                                                        "%I:%M%p").time())
        elif day.split(' ')[0] in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Aug', 'Nov', 'Dec']:
            month_of_game = datetime.datetime.strptime(day.split(' ')[0], "%b").month
            month_of_today = self.now_timestamp.month
            # Game is this year
            if month_of_game >= month_of_today:
                year_of_game = self.now_timestamp.year
            else:
                year_of_game = self.now_timestamp.year + 1

            if len(day.split(' ')) == 2:
                month_and_day = datetime.datetime.strptime(day, "%b %d")
                date = datetime.datetime(year_of_game, month_and_day.month, month_and_day.day, 20, 30)
            else:
                date_without_tz = day[slice(0, -3)] + " " + str(year_of_game)
                date = datetime.datetime.strptime(date_without_tz, "%b %d %I:%M %p %Y")
        else:
            today_day_of_wk = self.now_timestamp.isoweekday()
            # For some reason it thinks %A is nov ?
            game_day_of_wk = time.strptime(day.split(" ")[0], "%A").tm_wday + 1
            game_time = datetime.datetime.strptime(day.split(' ')[1] + day.split(' ')[2], "%I:%M%p").time()
            if today_day_of_wk < game_day_of_wk:
                days_ahead = game_day_of_wk - today_day_of_wk
            else:
                days_ahead = 7 - self.now_timestamp.date().isoweekday() + game_day_of_wk
            day_of_game = self.now_timestamp.date() + datetime.timedelta(days=days_ahead)
            date = datetime.datetime.combine(day_of_game, game_time)
        return date

    # Create MD5 Hash column to identify unique row
    @staticmethod
    def hash_cols(passed_df, columns, name="hash"):
        new_df = passed_df.copy()

        def func(row, cols):
            col_data = []
            for col in cols:
                col_data.append(str(row.at[col]))

            col_combined = ''.join(col_data).encode()
            hashed_col = hashlib.sha256(col_combined).hexdigest()
            return hashed_col

        new_df[name] = new_df.apply(lambda row: func(row, columns), axis=1)
        return new_df

    # ****** HELPER FUNCTIONS END ******

    # ****** TRANSFORMATION FUNCTIONS START ******
    def transform_odds_list_to_dataframe_row(self, game_matchup, header):
        df = pd.DataFrame()
        df["game_date"] = [self.convert_odds_day_to_date(game_matchup[0])]
        df["away_team_full_name"] = [game_matchup[1]]
        df["home_team_full_name"] = [game_matchup[2]]
        df["away_team_code"] = [game_matchup[3]]
        df["home_team_code"] = [game_matchup[4]]
        df["header_or_week"] = [header]
        df["update_timestamp"] = self.now_timestamp
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
        df = self.hash_cols(df, col_names, name="hash")
        return df

    # Inputs: None, this method inherits a sport from its parent class.
    # Outputs: Pandas Dataframe with odds
    def get_odds(self):
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
            matchup_array = []
            matchup_array.append(matchup_info)
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

    def get_team_futures(self):
        # Following line should go through team_odds and player_odds
        team_odds_df = pd.DataFrame()
        team_odds = VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["future_types"][self.sport]["team_odds"]
        # Team Odds
        for i, odds_type in enumerate(team_odds["urls"]):
            html = BeautifulSoup(requests.get(
                VEGAS_INSIDER_SCHEMA["base_url"] + self.sport + '/odds/' + odds_type,
                self.headers).content, self.parser)
            css_grid_search = VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["css"]["grid"]
            css_team_search = VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["css"]["teams"]
            css_odds_search = VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["css"]["odds"]
            grid_html = html.find("div", class_=css_grid_search)
            if grid_html != None:
                teams = grid_html.find("div", class_=css_team_search).getText(",", strip=True).split(',')[::2]
                odds_html = grid_html.find_all("div", class_=css_odds_search)
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
                temp_team_odds["future_type"] = team_odds["types"][i]
                new_df = pd.DataFrame(zip(*futures_in_grid_array))
                temp_team_odds = pd.concat([temp_team_odds, new_df], axis=1, ignore_index=True)
                team_odds_df = pd.concat([team_odds_df, temp_team_odds], axis=0, ignore_index=True)
        col_names_to_hash = ["team_name", "future_type"] + VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["bookmakers"]
        if len(team_odds["types"]) > 0:
            team_odds_df.columns = col_names_to_hash
            team_odds_df = self.hash_cols(team_odds_df, col_names_to_hash, name="hash")
            team_odds_df["update_timestamp"] = self.now_timestamp
        return team_odds_df

    # Need to do this next.
    def get_player_futures(self):
        temp_player_odds = pd.DataFrame()
        player_odds = VEGAS_INSIDER_SCHEMA["table_types"]["futures"]["future_types"][self.sport]["player_odds"]
        pass

    # def get_matchups(self):
    #     pass
    #
    # def get_schedules(self):
    #     pass
    def return_timestamp(self):
        return self.now_timestamp


# This is entry point for gcloud functions.
def scraper_cloud_function(event, context):
    for sport in VEGAS_INSIDER_SCHEMA["sports"].keys():
        # Get odds
        odds_dataframe = VegasInsiderScraper(sport).get_odds()
        if not odds_dataframe.empty:
            save_data(sport, odds_dataframe, VEGAS_INSIDER_SCHEMA["sports"][sport]["table_names"]["prod_temp_odds"],
                      VEGAS_INSIDER_SCHEMA["sports"][sport]["table_names"]["prod_odds"])
            local_ts = VegasInsiderScraper(sport).return_timestamp()
            save_to_s3(odds_dataframe, local_ts, sport, 'odds')
        # Get futures
    for sport in VEGAS_INSIDER_SCHEMA["sports"].keys():
        team_futures_dataframe = VegasInsiderScraper(sport).get_team_futures()
        if not team_futures_dataframe.empty:
            save_data(sport, team_futures_dataframe,
                      VEGAS_INSIDER_SCHEMA["sports"][sport]["table_names"]["prod_temp_team_futures"],
                      VEGAS_INSIDER_SCHEMA["sports"][sport]["table_names"]["prod_team_futures"])
            local_ts = VegasInsiderScraper(sport).return_timestamp()
            save_to_s3(team_futures_dataframe, local_ts, sport, 'futures')

# https://nzenitram.medium.com/medium-lambda-and-me-or-how-i-export-medium-stories-to-my-website-148b599ad271
# https://stackoverflow.com/questions/58345773/how-to-save-my-scraped-data-in-aws-s3-bucket
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
# https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3
# https://stackoverflow.com/questions/66059356/list-all-objects-in-aws-s3-bucket-with-their-storage-class-using-boto3-python/66072127#66072127
# def s3_lister():
#     s3_resource = boto3.resource('s3')
#     bucket = s3_resource.Bucket('oddstracker')
#     for obj in bucket.objects.all():
#         print(obj.key)


# odds_scrape = VegasInsiderScraper('nfl').get_odds()
# ts = datetime.datetime.now(TIMEZONE)
# save_to_s3(odds_scrape, ts, 'nfl', 'odds')
