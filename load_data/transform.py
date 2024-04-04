from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, IntegerType, DateType, \
    TimestampType
from pyspark.sql.functions import lit, from_unixtime, col
from utility import get_path
import os
from zipfile import ZipFile
from pyspark.sql import SparkSession
from cross_cutting import config_manager


class Transform:
    def __init__(self):
        self.config = config_manager.ConfigManager("settings.ini")

    def unzip(self, symbol):
        path = get_path("spot", "klines", "daily", symbol, "1m")
        zip_files = [file for file in os.listdir(path) if file.endswith(".zip")]
        for file in zip_files:
            if file.endswith(".zip"):
                with ZipFile(path + file, 'r') as zip_ref:
                    zip_ref.extractall(path)
                    os.remove(path + file)

    def transform(self, symbol):
        spark = SparkSession.builder.config("spark.jars", "postgresql-42.7.3.jar").master(
            self.config.get_value('spark', 'host')).appName("transform").getOrCreate()
        path = get_path("spot", "klines", "daily", symbol, "1m")

        csv_files = [file for file in os.listdir(path) if file.endswith(".csv")]
        raw_df = None
        schema = StructType([
            StructField("start_time_numeric", LongType(), True),
            StructField("open_price", DoubleType(), True),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("close_price", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("close_time_numeric", LongType(), True),
            StructField("QV", DoubleType(), True),
            StructField("number_of_trades", LongType(), True),
            StructField("TBB", DoubleType(), True),
            StructField("TBQ", DoubleType(), True),
            StructField("I", IntegerType(), True)
        ])
        for file in csv_files:
            df = spark.read.csv(path + file, header=False, schema=schema)
            if raw_df is None:
                raw_df = df
            else:
                raw_df = raw_df.union(df)

        raw_df = raw_df.drop("I").drop("QV").drop("TBB").drop("TBQ")
        # TODO lit with eth id
        raw_df = raw_df.withColumn("symbol_id", lit(3)) \
            .withColumn("start_time", from_unixtime(col("start_time_numeric") / 1000, "yyyy-MM-dd HH:mm:ss.SSS")
                        .cast(TimestampType())) \
            .withColumn("close_time", from_unixtime(col("close_time_numeric") / 1000, "yyyy-MM-dd HH:mm:ss.SSS")
                        .cast(TimestampType()))

        db_conn = self.config.get_value('pg', 'conn')
        db_user = self.config.get_value('pg', 'user')
        db_password = self.config.get_value('pg', 'password')

        (raw_df.write.option("driver", "org.postgresql.Driver").
         option("user", db_user).option("password", db_password)
         .jdbc(url=db_conn, table="f_klines", mode="append"))

        spark.stop()
