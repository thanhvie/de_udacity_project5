from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    truncate_sql = """
                    TRUNCATE {}
                   """ 
    insert_sql = """
                INSERT INTO {}
                 {} 
                """
    

    @apply_defaults
    def __init__(self, redshift_conn_id = "", table = "", sql_query = "",
                 truncate_table = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        if self.truncate_table:
            formatted_truncate_sql = LoadDimensionOperator.truncate_sql.format(self.table)
            redshift.run(formatted_insert_sql)
            
        formatted_insert_sql = LoadDimensionOperator.insert_sql.format(self.table, self.sql_query)
        redshift.run(formatted_insert_sql)
        
        self.log.info('LoadDimensionOperator executed')
        