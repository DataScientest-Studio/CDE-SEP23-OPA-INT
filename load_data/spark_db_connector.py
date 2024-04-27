import config_manager


class SparkDBConnector:
    def __init__(self):
        self.config = config_manager.ConfigManager("settings.ini")

    def write_to_db(self, table_name, raw_df):
        db_conn = self.config.get_value('pg', 'conn')
        db_user = self.config.get_value('pg', 'user')
        db_password = self.config.get_value('pg', 'password')

        raw_df.write.option("driver", "org.postgresql.Driver") \
            .option("user", db_user).option("password", db_password) \
            .jdbc(url=db_conn, table=table_name, mode="append")
