from pyspark.sql.types import StructType, StructField, LongType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import lit, from_unixtime, col
from pyspark.sql import DataFrame as Dataframe

from spark_db_connector import SparkDBConnector
from utility import get_path
import os
from zipfile import ZipFile
from pyspark.sql import SparkSession
from cross_cutting import config_manager
from cross_cutting import technical_indicators as ti


class Transform:
    def __init__(self):
        self.config = config_manager.ConfigManager("settings.ini")
        self.spark_db_connector = SparkDBConnector()

    def unzip(self, symbol):
        path = get_path("spot", "klines", "daily", symbol, "1m")
        zip_files = [file for file in os.listdir(path) if file.endswith(".zip")]
        for file in zip_files:
            if file.endswith(".zip"):
                with ZipFile(path + file, 'r') as zip_ref:
                    zip_ref.extractall(path)
                    os.remove(path + file)

    def transform_and_load(self, symbol):
        spark = SparkSession.builder.config("spark.jars", "postgresql-42.7.3.jar").master(
            self.config.get_value('spark', 'host')).appName("transform").getOrCreate()
        path = get_path("spot", "klines", "daily", symbol, "1m")

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


        csv_files = [file for file in os.listdir(path) if file.endswith(".csv")]
        for file in csv_files:
            df = spark.read.csv(path + file, header=False, schema=schema)
            if raw_df is None:
                raw_df = df
            else:
                raw_df = raw_df.union(df)

        raw_df = raw_df.drop("I").drop("QV").drop("TBB").drop("TBQ")
        # TODO lit with eth id
        raw_df = raw_df.withColumn("symbol_id", lit(1)) \
            .withColumn("start_time", from_unixtime(col("start_time_numeric") / 1000, "yyyy-MM-dd HH:mm:ss.SSS")
                        .cast(TimestampType())) \
            .withColumn("close_time", from_unixtime(col("close_time_numeric") / 1000, "yyyy-MM-dd HH:mm:ss.SSS")
                        .cast(TimestampType()))

        avg_df = ti.avg_price(raw_df)

        avg_df = self.build_kpi(avg_df)

        kpis_df = avg_df.drop("volume")

        kpis_df.printSchema()

        self.spark_db_connector.write_to_db("f_klines", raw_df)
        self.spark_db_connector.write_to_db("f_dvkpi", kpis_df)

        spark.stop()

    def build_kpi(self, avg_df) -> Dataframe:

        ewma_50 = ti.ewma(50, avg_df)
        ewma_200 = ti.ewma(200, avg_df)
        ewma_250 = ti.ewma(250, avg_df)

        sma_50 = ti.simple_ma(50, avg_df)
        sma_200 = ti.simple_ma(200, avg_df)
        sma_250 = ti.simple_ma(250, avg_df)

        rsi_20 = ti.rsi(20, avg_df)
        rsi_50 = ti.rsi(50, avg_df)
        rsi_100 = ti.rsi(100, avg_df)


        avg_df = avg_df.drop("volume")
        kpi_dfs = [ewma_50, ewma_200, ewma_250, sma_50, sma_200, sma_250, rsi_20, rsi_50, rsi_100]
        for kpi_df in kpi_dfs:
            avg_df = avg_df.union(kpi_df.select("dvkpi_timestamp", "dvkpi_timestamp_numeric", "dvkpi_kpi", "dvkpi_kpi_value", "dvkpi_symbol_id"))
        return avg_df



t = Transform()
t.unzip("ETHEUR")
t.transform_and_load("ETHEUR")