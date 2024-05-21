import pyspark.sql.types
from pyspark.sql.functions import lit, from_unixtime, col
from pyspark.sql import DataFrame as Dataframe

from spark_db_connector import SparkDBConnector
from utility import get_path
import os
from zipfile import ZipFile
from pyspark.sql import SparkSession
import config_manager
import technical_indicators_spark as ti


class Transform:
    def __init__(self):
        self.config = config_manager.ConfigManager('/load_data/settings.ini')
        self.spark_db_connector = SparkDBConnector()

    def unzip(self, symbol):
        path = "/" + get_path("spot", "klines", "daily", symbol, "1m")
        zip_files = [file for file in os.listdir(path) if file.endswith(".zip")]
        for file in zip_files:
            if file.endswith(".zip"):
                with ZipFile(path + file, 'r') as zip_ref:
                    zip_ref.extractall(path)
                    # os.remove(path + file)

    def transform_and_load(self, symbol):
        spark = (SparkSession.builder.
                 master(self.config.get_value('spark', 'host')).
                 config("spark.jars", "/load_data/postgresql-42.7.3.jar").
                 appName("transform").
                 getOrCreate())

        path = get_path("spot", "klines", "daily", symbol, "1m")
        path = "/" + path

        raw_df = None
        schema = pyspark.sql.types.StructType([
            pyspark.sql.types.StructField("start_time_numeric", pyspark.sql.types.LongType(), True),
            pyspark.sql.types.StructField("open_price", pyspark.sql.types.DoubleType(), True),
            pyspark.sql.types.StructField("high_price", pyspark.sql.types.DoubleType(), True),
            pyspark.sql.types.StructField("low_price", pyspark.sql.types.DoubleType(), True),
            pyspark.sql.types.StructField("close_price", pyspark.sql.types.DoubleType(), True),
            pyspark.sql.types.StructField("volume", pyspark.sql.types.DoubleType(), True),
            pyspark.sql.types.StructField("close_time_numeric", pyspark.sql.types.LongType(), True),
            pyspark.sql.types.StructField("QV", pyspark.sql.types.DoubleType(), True),
            pyspark.sql.types.StructField("number_of_trades", pyspark.sql.types.LongType(), True),
            pyspark.sql.types.StructField("TBB", pyspark.sql.types.DoubleType(), True),
            pyspark.sql.types.StructField("TBQ", pyspark.sql.types.DoubleType(), True),
            pyspark.sql.types.StructField("I", pyspark.sql.types.IntegerType(), True)
        ])


        csv_files = [file for file in os.listdir(path) if file.endswith(".csv")]
        for file in csv_files:
            full_file_path = os.path.join('file:///', path, file)
            full_file_path = full_file_path.replace("file:////", "file:///")
            df = spark.read.csv(full_file_path, header=False, schema=schema)
            if raw_df is None:
                raw_df = df
            else:
                raw_df = raw_df.union(df)

        raw_df = raw_df.drop("I").drop("QV").drop("TBB").drop("TBQ")
        # TODO lit with eth id
        raw_df = raw_df.withColumn("symbol_id", lit(1)) \
            .withColumn("start_time", from_unixtime(col("start_time_numeric") / 1000, "yyyy-MM-dd HH:mm:ss.SSS")
                        .cast(pyspark.sql.types.TimestampType())) \
            .withColumn("close_time", from_unixtime(col("close_time_numeric") / 1000, "yyyy-MM-dd HH:mm:ss.SSS")
                        .cast(pyspark.sql.types.TimestampType()))

        avg_df = ti.avg_price(raw_df)

        avg_df = self.build_kpi(avg_df)

        kpis_df = avg_df.drop("volume")

        kpis_df.printSchema()

        kpis_df.show(250)

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

