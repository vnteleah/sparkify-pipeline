from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
- Utilizes the provided SQL helper class to run data transformations.
- Contains a SQL_statement on which to the SQL transformation takes place
and provides the target table onto which the data is loaded.
- Most importantly target tables are emptied prior to data loading.
"""

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="postgres",
                 source="",
                 table_name="",
                 *args, **kwargs):
        
        self.redshift_conn_id = redshift_conn_id
        self.source = source
        self.table_name = table_name
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Prepapring fact_table
        redshift_hook.run(f"TRUNCATE TABLE {self.table_name}")      
        self.log.info(f"{self.table_name} has been successfully emptied prior to data loading")
        
        # Preparing SQL statement
        SQL_statement = f"INSERT INTO {self.table_name} {self.source}"
        self.log.info(f"insert sql: {SQL_statement}")
        
        redshift_hook.run(SQL_statement)
        
        self.log.info(f'LoadFactOperator has successfully inserted data into fact_table: {self.table_name}')
