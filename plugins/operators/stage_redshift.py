from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION AS '{}'
    FORMAT as json '{}'
    """

    @apply_defaults
    #what's in it
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 region = "us-west-2",
                 copy_json="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)  #MAPPING 
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.copy_json = copy_json
      

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id) # using Hook to connect with redshift
        aws_hook = AwsHook(self.aws_credentials_id) # using AwsHook to connect using aws_credentials
        credentials =aws_hook.get_credentials()
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f'Loading S3 to Redshift Table Name: {self.table}')
        s3_render_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{s3_render_key}"
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.copy_json
        )
        
        self.log.info(f"performing query to copy data from {s3_path} to {self.table}")
        redshift.run(formatted_sql)
        
        
        





