from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sql_queries import TABLE_INSERT

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 select_sql_query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql_query = select_sql_query

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info("Inserting into fact table {}".format(self.table))
        formatted_sql = """
                        INSERT INTO {}
                        {}
                        """.format(self.table, self.select_sql_query)
        redshift.run(formatted_sql)
