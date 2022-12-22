"""
A sample Hello World server.
"""
import os

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
import pymysql

# https://scraper-backend-pbmrnudl7a-uc.a.run.app/
# pylint: disable=C0103
app = Flask(__name__)
app.config.from_object(__name__)

# enable CORS
CORS(app, resources={r'/*': {'origins': '*'}})

aws_master_username = os.environ.get('AWS_MASTER_USERNAME')
aws_master_pw = os.environ.get('AWS_MASTER_PW')
aws_rdb_endpoint = os.environ.get('AWS_RDS_DB_ENDPOINT')
aws_rdb_db_port = os.environ.get('AWS_RDS_DB_PORT')
aws_rdb_db = os.environ.get('AWS_RDS_DB')

db_url = "mysql+pymysql://" + aws_master_username + ":" + aws_master_pw + "@" + aws_rdb_endpoint + ":" + aws_rdb_db_port + "/" + aws_rdb_db
engine = create_engine(db_url)


def execute_query(query_text):
    with engine.connect() as connection:
        result = connection.execute(text(query_text))
    return jsonify({'result': [dict(row) for row in result]})


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
# https://hevodata.com/learn/flask-mysql/
# https://testdriven.io/blog/developing-a-single-page-app-with-flask-and-vuejs/
# https://console.cloud.google.com/cloud-resource-manager
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
#


@app.route('/query/tables', methods=['GET', 'POST'])
def get_list_of_tables():
    return execute_query("SHOW TABLES")


@app.route('/query/table', methods=['GET', 'POST'])
def get_table():
    return execute_query("SELECT * FROM " + request.json['table_name'] + '_odds' + " LIMIT 1")


if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')
