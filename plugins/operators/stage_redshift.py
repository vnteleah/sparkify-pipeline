from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import SqlQueries

"""
- Stage_redshift operator is able to stage JSON files from S3 to Amazon Redshift.
- It creates and runs a SQL COPY statement based on the parameters provided.
"""
class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, 
                 redshift_conn_id="postgres",
                 aws_conn_id="aws_credentials",
                 file_type="JSON",
                 s3_bucket="udacity-dend",
                 log_source="s3://udacity-dend/log_data",
                 log_path="s3://udacity-dend/log_json_path.json",
                 song_source="s3://udacity-dend/song_data",
                 ACCESS_KEY_ID="###########",
                 SECRET_ACCESS_KEY="#########",
                 region="us-west-2",
                 staging_events="",
                 staging_songs="",
                 *args, **kwargs):
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.file_type = file_type 
        self.s3_bucket = s3_bucket
        self.log_source = log_source
        self.log_path = log_path
        self.song_source = song_source
        self.ACCESS_KEY_ID = ACCESS_KEY_ID
        self.SECRET_ACCESS_KEY = SECRET_ACCESS_KEY
        self.region = region
        self.staging_events = staging_events
        self.staging_songs = staging_songs
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
    def execute(self, context):
        # connect to aws.
        aws_hook = AwsHook(aws_conn_id=self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        self.log.info('connected to aws')
        
        # connect to redshift.
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('connected to redshift')
        
        # copy data from s3 to staging_event table in redshift.
        copy_staging_events = ("""
        COPY staging_events
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region '{}'
        JSON '{}'
        compupdate off
        """).format(self.log_source,
                    credentials.access_key, 
                    credentials.secret_key,
                    self.region,
                    self.log_path)
        
        self.log.info(f"...copying sql: {copy_staging_events}")
        
        redshift_hook.run(copy_staging_events)
        
        # copy data from s3 to staging_songs table in redshift.
        copy_staging_songs = ("""
        COPY staging_songs
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region '{}'
        JSON 'auto'
        compupdate off
        """).format(self.song_source,
                    credentials.access_key, 
                    credentials.secret_key,
                    self.region)
        
        self.log.info(f"...copying sql: {copy_staging_songs}")
        
        redshift_hook.run(copy_staging_songs)
        
        self.log.info(f"StageToRedshiftOperator has successfully loaded files from s3 to staging table in redshift")
   
