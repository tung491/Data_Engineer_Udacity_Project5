from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, redshift_conn_id, aws_credentials_id,
                 table, s3_bucket, s3_key, json_path='auto',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.table = table

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials_id = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'

        formatted_sql = """
                       COPY {} FROM '{}'
                        ACCESS_KEY_ID '{}'
                        SECRET_ACCESS_KEY '{}'
                        JSON {}
                        region 'us-west-2' TRUNCATECOLUMNS
                        """.format(self.table, s3_path, credentials.access_key,
                                            credentials.secret_key)
        redshift.run(formatted_sql)
