from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

"""
- This DAG is capable to perform important ETL process, 
including extracting data from Amazon S3 to transforming the data using SQL transformations, 
and loading it on data warehouse (Amazon Redshift) and transforming the data.
- Important dependencies were designed to accomplish this:
# StageToRedshiftOperator - for extracting data from S3 to Redshift
# LoadFactOperator - for loading the fact table onto the fact tables
# LoadDimensionOperator - for loading the fact table onto the dimension tables
# DataQualityOperator - for controlling data quality at the end of the ETL process
# SqlQueries within the helpers folder - contains all SQL statements for transformation,
and also contains quality check SQL statements.
# create_tables.sql - contains all CREATE TABLE statements that are used to create tables in Amazon Redshift. 
"""

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '0 * * * *',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    redshift_conn_id = "postgres",
    aws_conn_id = "aws_credentials",
    source = "s3://udacity-dend/log_data",
    target_table = "staging_events",
    file_type = "s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    redshift_conn_id = "postgres",
    aws_conn_id = "aws_credentials",
    source = "s3://udacity-dend/song_data",
    target_table = "staging_songs",
    file_type = "s3://udacity-dend/song_json_path.json"
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = "postgres",
    source = SqlQueries.songplay_table_insert,
    table_name = "songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    redshift_conn_id = "postgres",
    source = SqlQueries.user_table_insert,
    table_name = "users",
    truncate = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    redshift_conn_id = "postgres",
    source = SqlQueries.song_table_insert,
    table_name = "songs",
    truncate = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    redshift_conn_id = "postgres",
    source = SqlQueries.artist_table_insert,
    table_name = "artists",
    truncate = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    redshift_conn_id = "postgres",
    source = SqlQueries.time_table_insert,
    table_name = "time",
    truncate = True
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    redshift_conn_id = "postgres",
    SQL_quality_test = [
        {"songplay_qc": SqlQueries.songplay_table_QC, "expect": 0},
        {"user_qc": SqlQueries.user_table_QC, "expect": 0},
        {"song_qc": SqlQueries.song_table_QC, "expect": 0},
        {"artist_qc": SqlQueries.artist_table_QC, "expect": 0},
        {"time_qc": SqlQueries.time_table_QC, "expect": 0}
    ]
)

end_operator = DummyOperator(task_id = 'Stop_execution',  dag = dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
