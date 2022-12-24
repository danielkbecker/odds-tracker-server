"""
This is a flask app which serves as a REST API for the database. It's only purpose is to receive requests
 and return the results of the queries to the frontend via pre-configured endpoints.
"""
import os

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
from pytz import timezone
import time
import datetime
import pymysql


# pylint: disable=C0103
app = Flask(__name__)
app.config.from_object(__name__)

# enable CORS
CORS(app, resources={r'/*': {'origins': '*'}})

# AWS Credentials are best stored in environment variables
aws_master_username = os.environ.get('AWS_MASTER_USERNAME')
aws_master_pw = os.environ.get('AWS_MASTER_PW')
aws_rdb_endpoint = os.environ.get('AWS_RDS_DB_ENDPOINT')
aws_rdb_db_port = os.environ.get('AWS_RDS_DB_PORT')
aws_rdb_db = os.environ.get('AWS_RDS_DB')

# Timezone for the server
tz = timezone('EST')

# The database engine which is used to connect to the database
db_url = "mysql+pymysql://" + aws_master_username + ":" + aws_master_pw + "@" + aws_rdb_endpoint + ":" + aws_rdb_db_port + "/" + aws_rdb_db
engine = create_engine(db_url)


# This helper function receives the text of a query as an input and returns the results of the query in JSON format.
def execute_query(query_text):
    with engine.connect() as connection:
        result = connection.execute(text(query_text))
    return {'result': [dict(row) for row in result]}


# The following are the endpoints which are used to receive requests from the frontend and
# return the results of the queries in JSON format.

# This route is a dummy route and can likely be removed.
@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    message = "It's running!"

    """Get Cloud Run environment variables."""
    service = os.environ.get('K_SERVICE', 'Unknown service')
    revision = os.environ.get('K_REVISION', 'Unknown revision')

    return render_template('index.html',
                           message=message,
                           Service=service,
                           Revision=revision)


# Quick change
# This is a test route which can be used to test the connection of the frontend to this flask application.
@app.route('/ping', methods=['GET'])
def ping_pong():
    return jsonify('pong!')


# @app.route('/<String: sport>/odds/', methods=['GET', 'POST'])
# def get_odds_for_sport(sport):
#     query_text = "SELECT * FROM " + request.json['sport'] + "_odds LIMIT 10"
#     return execute_query(query_text)

# @app.route('/<sport>/futures/', defaults={'sport', 'nfl'}, methods=['GET', 'POST'])
# def get_futures_for_sport(sport):
#     query_text = "SELECT * FROM " + request.json[sport] + "_odds LIMIT 10"
#     return execute_query(query_text)

# This route is used to get the odds for the games which are happening today across all of the sport which are tracked.
@app.route('/today', methods=['GET'])
def get_todays_matchups():
    current_timestamp = datetime.datetime.now(tz)
    print(current_timestamp)
    query_strings = {
        "nfl": '',
        "nba": '',
        "nhl": '',
    }
    todays_odds = {
        "nfl": None,
        "nba": None,
        "nhl": None,
    }
    sports_list = ['nfl', 'nba', 'nhl']
    for sport in sports_list:
        query_strings[sport] = "SELECT * FROM " + sport + "_odds WHERE year(game_date) >= " + str(current_timestamp.year) + " AND " + "month(game_date) >= " + str(current_timestamp.month) + " AND " + "day(game_date) >= " + str(current_timestamp.day) + " AND " + "hour(game_date) >= " + str(current_timestamp.hour)
        todays_odds[sport] = execute_query(query_strings[sport])
    return jsonify(todays_odds)


@app.route('/query/tables', methods=['GET', 'POST'])
def get_list_of_tables():
    return jsonify(execute_query("SHOW TABLES"))


@app.route('/query/table', methods=['GET', 'POST'])
def get_table():
    return jsonify(execute_query("SELECT * FROM " + request.json['table_name'] + '_odds' + " LIMIT 1"))


if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')


