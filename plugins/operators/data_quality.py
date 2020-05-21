from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    @staticmethod
    def check_rows(redshift, table):
        query = f"""SELECT COUNT(*)
        FROM {table}
        """
        records = redshift.get_records(query)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality checks in {table} fail. "
                             f"Because this table don't have any data")
        self.log.info(f"{table} pass the quality check")

    def execute(self, context):
        if len(tables > 0):
            redshift = PostgresHook(self.redshift_conn_id)
            for table in self.tables:
                check_rows(redshift, table)


