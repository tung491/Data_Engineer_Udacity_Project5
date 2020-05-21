from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 select_sql_query,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql_query = select_sql_query

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info("Inserting into dimension table {}".format(self.table))

        formatted_sql = """
                        INSERT INTO {}
                        {}
                        """.format(self.table, self.select_sql_query)

        redshift.run(formatted_sql)
