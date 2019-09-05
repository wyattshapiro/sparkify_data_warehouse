
# Data Warehouse with Redshift for Sparkify

## Goal

Sparkify has been collecting on songs and user activity for their new music streaming app. One of their teams is interested in analyzing their users listening habits, which could help them classify their users for targeted ads, create/improve a recommendation algorithm for songs, and more. Their company has grown enough that they wish to move all processes onto the cloud.

Note: This project is for a fictional company and is part of Udacity's Data Engineering Nanodegree.

### Problem

Currently, Sparkify's user history and song data is stored on S3 in a directory of JSON log formats.
This format does not lend itself well to analysis.

### Solution

In order to enable a team to effectively gain insight from user activity, a data engineer needs to structure the data and load it into a database. The proposed plan consists of a Python ETL pipeline that will:

- Extract each JSON file hosted on S3.
- Load data into staging tables on Redshift.
- Transform and load data into analytics tables with star schema on Redshift.
- Execute SQL queries on analytics tables.

## Data

### Song Dataset

- This dataset is a subset of real song data from the Million Song Dataset.
- Files live on S3 with the link s3://udacity-dend/song_data
- Each file is in JSON format and contains metadata about a song and the artist of that song.
  - The files are partitioned by the first three letters of each song's track ID. For example:
    - song_data/A/B/C/TRABCEI128F424C983.json
    - song_data/A/A/B/TRAABJL12903CDCF1A.json

### Log Dataset

- This dataset is event data generated by an event simulator based on the songs in the dataset above.
- Files live on S3 with the link s3://udacity-dend/log_data
- Each file is in JSON format and contains data about user events in the app.
  - The files are partitioned by year and month. For example:
    - log_data/2018/11/2018-11-12-events.json
    - log_data/2018/11/2018-11-13-events.json


## Data Models

### Entities

The database is structured as a star schema for analysis of song plays. As such, the fact table (ie center of the star) will be songplays, and it will have it's associated dimensions related as foreign keys.

Fact table
- songplays: records in log data associated with song plays

Dimension tables
- app_users: users in the app
- songs: songs in music database
- artists: artists in music database
- time: timestamps of records in songplays broken down into specific units

### Entity Relationship Diagram (ERD)

![Alt text](sparkify_ERD.png?raw=true "Sparkify ERD")


## Installation

Clone the repo onto your machine with the following command:

$ git checkout https://github.com/wyattshapiro/sparkify_data_warehouse.git


## Dependencies

I use Python 3.7.

See https://www.python.org/downloads/ for information on download.

----

I use virtualenv to manage dependencies, if you have it installed you can run
the following commands from the root code directory to create the environment and
activate it:

$ python3 -m venv venv

$ source venv/bin/activate

See https://virtualenv.pypa.io/en/stable/ for more information.

----

I use pip to install dependencies, which comes installed in a virtualenv.
You can run the following to install dependencies:

$ pip install -r requirements.txt

See https://pip.pypa.io/en/stable/installing/ for more information.

----

I use AWS S3 and Redshift for data storage and processing.

See https://aws.amazon.com/ for more information.


## Usage

There are several main scripts:

- src/dwh.cfg.sample: Specifies required environmental variables for AWS connections. Note: Actual file is dwh.cfg but it is in .gitignore to prevent exposing real credentials.
- src/create_tables.py: Drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
- src/etl.py: Reads and processes *all files* from song_data and log_data and loads them into your tables.
- src/analyze_tables.py: Runs queries on Redshift cluster to surface analytical insights.
- src/sql_queries.py: Contains all sql queries, and is used during ETL process and analysis.

**Steps to run**
1. Navigate to top of project directory
2. Create virtualenv (see Dependencies)
3. Activate virtualenv (see Dependencies)
4. Install requirements (see Dependencies)
5. Configure src/dwh.cfg for AWS
6. $ python3 src/create_tables.py
7. $ python3 src/etl.py
8. $ python3 src/analyze_tables.py

## Analysis

In src/analyze_tables.py, I created several example queries. These queries showed that:

- Users played bands like Muse, The Smiths, and Radiohead the most.
- Users are spread across the US - Cleveland, Ohio to LA.

## Future Optimizations

- Add genre data to song table
- Add data quality checks
- Create a dashboard for analytic queries
