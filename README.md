# Sparkify Pipeline

## Objective
In this project I aimed to extract data from S3, perform ETL and return a star schema on AWS Redshift for ***Sparkify**. They have been collecting user and song activity on their new music streaming app, and have employed the skills of data analysts who are particularly interested in knowing what songs users are listening to. They have grown their user base and song database even more and want to move their data warehouse to a data lake. As such my skills as a data engineer was employed.

**Sparkify is a fictional startup and music streaming platform.*

## Data Source
- Log dataset is a json formate dataset, derived from [Eventsim](https://github.com/Interana/eventsim). Eventsim is a program that simulates activity logs from a music streaming app for testing and demo purposes. 

- Song dataset is a json formate dataset, that originates from a freely-available collection of audio features and metadata for a million contemporary popular music tracks at [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).

## Method

1. Setup an AWS IAM role that grants the necessary access to extract, read and load on AWS S3.
       
2. With the aid of my python script, perform the accomplish the following:
- develop a pipeline that connects to Sparkify's AWS S3 bucket and extract data from S3.
- parse JSON formatted files from there into comprehensive dataframes.
- more importantly prepare a star schema comprised of:
    - fact table: songplays
    - four dimensional tables: users, songs, artists, and time

## Result

### Star Schema
Fact Table.
1. songplays - records in log data associated with song plays i.e. records with page NextSong.
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.
Dimension Tables.
2. users - users in the app.
    - user_id, first_name, last_name, gender, level.
3. songs - songs in music database.
    - song_id, title, artist_id, year, duration.
4. artists - artists in music database.
    - artist_id, name, location, latitude, longitude.
5. time - timestamps of records in songplays broken down into specific units.
    - start_time, hour, day, week, month, year, weekday.

![Star schema](https://github.com/vnteleah/sparkify-pipeline/blob/main/Sparkify_schema.png "Star Schema")


### Airflow DAG

            |--Readme.md
            |--dags
                  |--dag.py # python script containing the tasks and dependencies of the DAG
            |--plugins
                  |--__init__.py
                  |--operators
                        |--__init__.py
                        |--data_quality.py # python script with data checks.
                        |--load_dimension.py # python script that loads dimension tables to aws redshift.
                        |--load_fact.py # python script that loads fact table to aws redshift.
                        |--stage_redshift.py # python script that stages tables to aws redshift prior to loading on fact and dimension tables.
                  |--helpers
                        |--__init__.py
                        |--sql_quaries.py # Defining SQL_queries
            |--create_tables.sql # sql script with all tables and datatype to be created


## Installations and usage

### Add DAG to Airflow
1. Before proceeding, ensure you've installed Airflow locally.
2. Follow the instructions from the Airflow installer manual on to add DAG's to Airflow.
3. Once the DAG is added to airflow, configure airflow connection as described next. 

### Add Airflow Connections
Configure AWS credentials and connection to Redshift on Airflow's UI.
1. Click on the Admin tab and select Connections.
2. Under Connections, select Create.
3. On the create connection page, enter the following values:

- Conn Id: Enter aws_credentials.
- Conn Type: Enter Amazon Web Services.
- Login: Enter your Access key ID from the IAM User credentials.
- Password: Enter your Secret access key from the IAM User credentials.

Once you've entered these values, select Save and Add Another.

4. On the next create connection page, enter the following values:

- Conn Id: Enter redshift.
- Conn Type: Enter Postgres.
- Host: Enter the endpoint of your Redshift cluster, excluding the port at the end.
- Schema: Enter **dev**. This is the Redshift database you want to connect to.
- Login: Enter user name created when launching your Redshift.
Password: Enter the password you created when launching your Redshift cluster.
- Port: Enter **5439**.

Once you've entered these values, select Save.

Run the Dag on Airflow UI

![Working DAG with task dependencies](https://github.com/vnteleah/sparkify-pipeline/blob/main/Sparkify_dag.png "Working DAG with task dependencies")
