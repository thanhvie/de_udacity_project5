from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    drop_table_template = """
                          DROP TABLE IF EXISTS {}
                          """
    
    fact_table_template = """
                          CREATE TABLE {} AS
                          {}
                          """
    

    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", sql_query ="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        drop_sql = LoadFactOperator.drop_table_template.format(self.table)
        redshift.run(drop_sql)
        fact_sql = LoadFactOperator.fact_table_template.format(self.table, self.sql_query)
        redshift.run(fact_sql)
        self.log.info('LoadFactOperator executed')