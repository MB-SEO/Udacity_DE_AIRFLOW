from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow import conf
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateTableOperator, StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator


AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 8, 17),
    'depends_on_past' : False,
    'email_on_retry': False,
    'email_on_failure': ['hs004457@gmail.com'],
    'retries' : 3,
    'retry_delay' : timedelta(minutes = 5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables = CreateTableOperator(
    task_id = 'create_tables_in_redshift',
    redshift_conn_id = 'redshift',
    dag = dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log-data',
    copy_json='s3://udacity-dend/log_json_path.json',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key = 'song-data',
    copy_json='auto'
)
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.user_table_insert,
    truncate = True
    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.song_table_insert,
    truncate = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.artist_table_insert,
    truncate = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.time_table_insert,
    truncate = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    quality_check_queries = [
        { 'quality_check': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL'}, 
        { 'quality_check': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL'},
        { 'quality_check': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL'},
        { 'quality_check': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL'}
    ])

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

    
      
# task ordering
        
start_operator >> create_tables 
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

stage_events_to_redshift >>load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >>load_user_dimension_table
load_songplays_table >>load_song_dimension_table
load_songplays_table >>load_artist_dimension_table
load_songplays_table >>load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
        
