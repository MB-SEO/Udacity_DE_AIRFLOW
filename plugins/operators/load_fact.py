from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_insert = """
    INSERT INTO {}
    {};
    """

    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_conn_id = '',
                 sql_query = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        formatted_sql =LoadFactOperator.sql_insert.format(
            self.table,
            self.sql_query
        )
        
        self.log.info(f'Loading {self.table} Table')
        redshift.run(formatted_sql)
