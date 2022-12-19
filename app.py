"""
A sample Hello World server.
"""
import os

from flask import Flask, render_template, jsonify
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
@app.route('/pingpong', methods=['GET'])
def ping_ponger():
    return jsonify('ping pong!')


@app.route('/ping', methods=['GET'])
def ping_pong():
    return jsonify('pong!')


@app.route('/query/tables', methods=['GET'])
def get_list_of_tables():
    with engine.connect() as connection:
        sql_string = text("SHOW TABLES")
        result = connection.execute(sql_string)
    return jsonify({'result': [dict(row) for row in result]})


@app.route('/query/nhl_odds', methods=['GET'])
def query():
    with engine.connect() as connection:
        sql_string = text("SELECT * FROM nhl_odds")
        result = connection.execute(sql_string)
    return jsonify({'result': [dict(row) for row in result]})


if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')
