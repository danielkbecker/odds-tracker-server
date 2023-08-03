# Odds Tracker Backend

This repository serves and deploys two different "microservices" 

   - Scrapes betting data in 15 minute intervals from VegasInsider.com and stores it in an AWS RDS MySQL database. The scraper runs using a Google Cloud Function and is triggered by a Cloud Scheduler job using a Pub/Sub subscription. The scraper is written in Python and uses the BeautifulSoup library to parse the HTML of the website.
   - Provides a REST API through a Flask App that can be used to query the database from any other front-end application. The Flask App is deployed through Google Cloud Run, which "is an ideal serverless platform for stateless containerized microservices that don’t require Kubernetes features like namespaces, co-location of containers in pods (sidecars) or node allocation and management."
    
## Getting Started
1) Create a Free Tier AWS Account
    - Create a MySQL database using AWS RDS
    - Create an S3 Storge Bucket

2) Create a Google Cloud Platform Account
    - Create a Cloud Scheduler Job
    - Create a Pub/Sub Subscription
    - Create a Cloud Function
    - Create a Cloud Run Service

Local Development Requirements:
    - Pycharm IDE
    - Python 3.10 Project Interpreter
    - Envfile Pycharm Plugin
    - Google Cloud SDK and CLI with necessary credentials (free tier)
    - AWS SDK and CLI with necessary credentials (free tier)
    - MySQL Workbench

## Scraper Setup
Set up your S3 bucket if you don't have one. Set up a MySQL Database if you don't have one.
This project uses an AWS RDS MySQL free tier database, but any database host will work.

MYSQL_USERNAME=admin
MYSQL_PW= ***** HIDING FOR LENNAR *****
MYSQL_HOST=database-1.abcd.us-east-1.rds.amazonaws.com
MYSQL_DB_NAME = sportsoddsdb
MYSQL_DB_PORT = 3306
S3_ACCESS_KEY_ID = S3ACCESSKEYID
S3_SECRET_ACCESS_KEY = S3ACCESSKEY
S3_BUCKET_ENDPOINT = https://PROJECTNAME.s3.amazonaws.com/FOLDER/

To deploy the scraper through Goo
Deploy: gcloud functions deploy scrape_vegas_insider --timeout=540

Config: 


## 
Run testing: pytest


### Run the app locally with the Cloud Run Emulator
#### Define run configuration

1. Click the Run/Debug configurations dropdown on the top taskbar and select 'Edit Configurations'
![image](./img/edit-config.png)

2. Click Cloud Code: Cloud Run -> Run Local Server


### Deploy the app to Google Cloud Run
#### Define run configuration

1. Click the Run/Debug configurations dropdown on the top taskbar and select 'Edit configurations'