from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 quality_check_queries = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_check_queries = quality_check_queries

    def execute(self, context):
        num_checks = len(self.quality_check_queries)
        if num_checks <= 0:
            self.log.info('NO QUALITY CHECK PERFORMED')
        else:
            self.log.info(f'Data Quality check processed {num_checks} times')
           
        redshift_hook= PostgresHook(self.redshift_conn_id)
        
        for query in quality_check_queries:
            query_format = query.get('quality_checks')
            record = redshift_hook.get_records(query_format)[0]
            if len(record) > 0:
                self.log.info(f'query:{query_format} did not pass the quality check')
                raise ValueError(f'data quality check for {query_format} failed')
            else:
                self.log.info(f'data quality check for {query_format} passed')
        