import json
import lxml
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
from utils.scraper import Scraper
import utils.helpers as helper_lib

# import datetime
# import time
# import numpy as np
from pytz import timezone


# Class inherits URL Headers, Parser, Local Timezone, and Timestamp from Base Class
class VegasInsider(Scraper):
    def __init__(self):
        # Define timestamp
        self._timestamp = datetime.datetime.now(timezone('EST'))
        # Config file loaded and vars defined from config file
        config_file = open('cloud_functions/scrape/vegas_insider/website_config.json', 'r')
        self.config = json.load(config_file)['vegas_insider']
        self.sports = self.config['sports']
        self.tables = self.config['tables']
        self.base_url = self.config['base_url']
        self.bookmakers = self.config['bookmakers']
        # Vars defined from base class. These could be new subclasses of the base class.
        self.table_subtypes = None
        self.sport = None
        self.table = None
        # Inheriting some BS4 stuff from the base class
        super().__init__()

    @staticmethod
    def convert_odds_day_to_date(ts, day):
        if day == 'Live':  # or
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
            today_day_of_wk = ts.isoweekday()
            game_day_of_wk = time.strptime(day.split(" ")[0], "%A").tm_wday + 1
            game_time = datetime.datetime.strptime(day.split(' ')[1] + day.split(' ')[2], "%I:%M%p").time()
            if today_day_of_wk < game_day_of_wk:
                days_ahead = game_day_of_wk - today_day_of_wk
            else:
                days_ahead = 7 - ts.date().isoweekday() + game_day_of_wk
            day_of_game = ts.date() + datetime.timedelta(days=days_ahead)
            date = datetime.datetime.combine(day_of_game, game_time)
        return date

    @staticmethod
    def find_bookmaker_in_string(string):
        url = string[string.find("(") + 1:string.find(")")]
        bookmaker_name = url[url.rfind("/") + 1:url.rfind(".")] if not any(char.isdigit() for char in url) else \
            url.rsplit('logo/')[1].rsplit('/')[0]
        return bookmaker_name

    def odds_table_logic(self, subtype_container_tag, css, title, bookmakers):
        odds_array = []
        matchups = subtype_container_tag.find_all('div', class_=css['matchups'])
        for matchup_index, matchup in enumerate(matchups):
            matchup_info = matchup.find("div", class_=css['matchup_info']). \
                getText(",", strip=True).replace(',Bet Now', '')
            matchup_array = [matchup_info]
            matchup_rows = matchup.find_all('div', class_=css['row_classes'])
            for matchup_row in matchup_rows:
                odds_boxes = matchup_row.find_all('div', class_=css['odds_box'])
                for odds_box in odds_boxes:
                    in_box = odds_box.getText(",", strip=True).replace(',Bet Now', '')
                    if len(in_box) == 0:
                        matchup_array.append("N/A")
                    else:
                        matchup_array.append(in_box)
            matchup_array = ','.join(matchup_array)
            odds_array.append(matchup_array)
        " ".join(odds_array)
        odds_df_array = []
        for matchup in odds_array:
            split_matchup = matchup.split(',')
            if split_matchup[0] != 'Final' and split_matchup[0] != 'Canceled':
                temp_df = self.transform_odds_list_to_dataframe_row(split_matchup, title, css, bookmakers)
                odds_df_array.append(temp_df)
        if len(odds_df_array) > 0:
            odds_table_dataframe = pd.concat(odds_df_array, ignore_index=True)
        else:
            odds_table_dataframe = pd.DataFrame()

        return odds_table_dataframe

    @staticmethod
    def team_futures_table_logic(subtype_container_tag, css, title, bookmakers, subtype, teams):
        team_futures_df = pd.DataFrame()
        if subtype_container_tag:
            futures_in_grid_array = []
            future_cols = subtype_container_tag.find_all("div", class_="d-flex flex-column")
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
            # This is probably wrong
            temp_team_odds["future_type"] = subtype
            new_df = pd.DataFrame(zip(*futures_in_grid_array))
            temp_team_odds = pd.concat([temp_team_odds, new_df], axis=1, ignore_index=True)
            team_futures_df = pd.concat([team_futures_df, temp_team_odds], axis=0, ignore_index=True)
        print(team_futures_df)
        return team_futures_df

    def transform_odds_list_to_dataframe_row(self, game_matchup, header, css, bookmakers):
        df = pd.DataFrame()
        df["game_date"] = [self.convert_odds_day_to_date(self._timestamp, game_matchup[0])]
        df["away_team_full_name"] = [game_matchup[1]]
        df["home_team_full_name"] = [game_matchup[2]]
        df["away_team_code"] = [game_matchup[3]]
        df["home_team_code"] = [game_matchup[4]]
        df["header_or_week"] = [header]
        df["update_timestamp"] = self._timestamp
        bet_types = self.config['sports'][self.sport]['bet_order']
        away_home_order = ["away", "home"]

        col_names = []
        col_index = 5
        temp_array = []

        for bet_type_index, bet_type in enumerate(bet_types, start=1):
            for home_away_index, home_or_away in enumerate(away_home_order, start=1):
                for bookmaker_index, bookmaker in enumerate(bookmakers,
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

    def get_dataframe_from_container(self, subtype_container_tag, bookmakers, css, title, subtype, teams):
        final_df = pd.DataFrame()
        if subtype_container_tag:
            if self.table == 'odds':
                final_df = self.odds_table_logic(subtype_container_tag, css, title, bookmakers)
            elif self.table == 'team_futures':
                final_df = self.team_futures_table_logic(subtype_container_tag, css, title, bookmakers, subtype, teams)
            elif self.table == 'player_futures':
                final_df = pd.DataFrame()
            else:
                pass
            return final_df
        else:
            return final_df

    def transform_subtype_to_dataframe(self, soup, subtype):
        # Get the title of the table from the soup object
        table_title = soup.find('h1').text
        # Get the css selectors for the table from the config file
        css = self.tables[self.table]['css']
        container = soup.find('div', class_=css['container'])
        if container:
            print(self.sport, self.table, subtype, table_title)
            # Get the y-axis (rows) of the table from the soup object (teams)
            rows = [team.getText(",", strip=True) for team in container.find_all('div', class_=css['teams'])]
            # cols = container.find('div', class_=css['bookmakers']['outer'])
            if rows:
                # Get the x-axis (columns) of the table from the soup object (bookmakers)
                bookmakers_soup = container.find_all('div', class_=css['bookmakers']['inner']) if rows else pd.DataFrame()
                columns = [self.find_bookmaker_in_string(bookmaker.get('style')) for bookmaker in bookmakers_soup if
                           bookmakers_soup]
                df = self.get_dataframe_from_container(container, columns, css, table_title, subtype, rows)
            else:
                df = pd.DataFrame()
            converted_df = df
        else:
            print(f'No data found for {self.sport} {self.table} {subtype} table.')
            converted_df = pd.DataFrame()
        return converted_df

    def extract_table(self):
        table_soup = []
        for item in self.table_subtypes:
            url = f"{self.config['base_url']}/{self.sport}" \
                  f"{self.tables[self.table]['source_url']}{item[list(item.keys())[0]]['url']}"
            print(f"Scraping {url}...")
            subtype_soup = self.get_soup(url)
            table_soup.append(subtype_soup)
        return table_soup

    # Table is a list of soup objects. Each soup object represents the raw html soup for each subtype of the table.
    # Ex: table = [soup1, soup2, soup3, ...]
    def transform_table(self, table_soup):
        transformed_table = pd.DataFrame()
        print(f"Converting {self.sport}_{self.table} soup to dataframe...")
        for (subtype_order, subtype) in enumerate(self.table_subtypes):
            subtype_df = self.transform_subtype_to_dataframe(table_soup[subtype_order], subtype)
            transformed_table = pd.concat([transformed_table, subtype_df])
        return transformed_table

    def load_table(self, table):
        return self, table
        pass

    # Perform the ETL routine to scrape the data from this website.
    def scrape(self):
        for sport in self.sports:
            self.sport = sport
            for table in self.tables:
                self.table = table
                print(f"Fetching updates to {sport}_{table}...")
                self.table_subtypes = self.config["sports"][sport]["tables"][table]["subtypes"]
                if self.table_subtypes:
                    extracted_table = self.extract_table()
                    transformed_table = self.transform_table(extracted_table)
                    print(transformed_table)
                    load_table = self.load_table(transformed_table)
                    # print(load_table is not None)
