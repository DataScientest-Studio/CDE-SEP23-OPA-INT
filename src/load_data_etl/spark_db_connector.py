import etl_settings


class SparkDBConnector:


    def write_to_db(self, table_name, raw_df):
        db_conn = etl_settings.pg['conn']
        db_user = etl_settings.pg['user']
        db_password = etl_settings.pg['password']

        raw_df.write.option("driver", "org.postgresql.Driver") \
            .option("user", db_user).option("password", db_password) \
            .jdbc(url=db_conn, table=table_name, mode="append")