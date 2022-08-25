from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_insert = """
    INSERT INTO {}
    {}
    """
    sql_truncate = """
    TRUNCATE TABLE {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql_query = '',
                 truncate = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshit_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        redshift.run(LoadDimensionOperator.sql_truncate.format(self.table))
        self.log.info(f'Truncating {self.table}')
                      
        sql_formatted = LoadDimensionOperator.sql_insert.format(self.table, self.sql_query)
                     
        self.log.info(f'executing query to insert data to {self.table}')
        redshift.run(sql_formatted)
