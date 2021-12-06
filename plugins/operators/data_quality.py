from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
- This operator is used to run checks on the data itself.
In order to accomplish this an expected result is provided, which sould match with the actual outcome from the DAG.
"""

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "postgres",
                 SQL_quality_test = [],                 
                 *args, **kwargs):

        self.redshift_conn_id = redshift_conn_id
        self.SQL_quality_test = SQL_quality_test
        super(DataQualityOperator, self).__init__(*args, **kwargs)
       
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for (SQL_statement, expected_result) in self.SQL_quality_test:
            row = redshift_hook.get_first(SQL_statement)
            if row is not NULL:
                if row[0] == expected_result:
                    self.log.info(f"Passed Test: {SQL_statement}\nResult == {expected_result}")
                else:
                    raise ValueError(f"Failed Test: {SQL_statement}\nResult != {expected_result}")
                   
        self.log.info("DataQualityOperator successfully completed quality check")
    
    