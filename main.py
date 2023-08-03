# THIS IS THE MAIN CLOUD FUNCTION FILE.
from cloud_functions.scrape.scrape_routine import scrape_routine
import os

# Import Environment Variables
MYSQL_USERNAME = os.environ["MYSQL_USERNAME"]
MYSQL_PW = os.environ["MYSQL_PW"]
MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_DB_NAME = os.environ["MYSQL_DB_NAME"]
MYSQL_DB_PORT = os.environ["MYSQL_DB_PORT"]
S3_ACCESS_KEY_ID = os.environ["S3_ACCESS_KEY_ID"]
S3_SECRET_ACCESS_KEY = os.environ["S3_SECRET_ACCESS_KEY"]
S3_BUCKET_ENDPOINT = os.environ["S3_BUCKET_ENDPOINT"]

env_vars = {
    "MYSQL_USERNAME": MYSQL_USERNAME,
    "MYSQL_PW": MYSQL_PW,
    "MYSQL_HOST": MYSQL_HOST,
    "MYSQL_DB_NAME": MYSQL_DB_NAME,
    "MYSQL_DB_PORT": MYSQL_DB_PORT,
    "S3_ACCESS_KEY_ID": S3_ACCESS_KEY_ID
}


# Right now the normal routine for the cloud function is to scrape, but it could do more later.
# Cloud Function Entry Point is a wrapper for the routine function, which could also serve as an entry point if needed.
def cloud_function(event, context):
    print(event, context)
    print("Scraper Started")
    scrape_routine()


# This is the entry point for the cloud function.
cloud_function('', '')
