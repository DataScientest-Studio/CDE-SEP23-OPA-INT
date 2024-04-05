from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, IntegerType, DateType, \
    TimestampType
from pyspark.sql.functions import lit, from_unixtime, col, mean
from utility import get_path
import os
from zipfile import ZipFile
from pyspark.sql import SparkSession, DataFrame
from cross_cutting import config_manager
from cross_cutting import technical_indicators as ti


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
        raw_df = raw_df.withColumn("symbol_id", lit(3)) \
            .withColumn("start_time", from_unixtime(col("start_time_numeric") / 1000, "yyyy-MM-dd HH:mm:ss.SSS")
                        .cast(TimestampType())) \
            .withColumn("close_time", from_unixtime(col("close_time_numeric") / 1000, "yyyy-MM-dd HH:mm:ss.SSS")
                        .cast(TimestampType()))

        raw_df.printSchema()
        avg_df = self.prepare_avg_price(raw_df)

        avg_df = self.build_kpi(avg_df)

        avg_df = avg_df.drop("volume")
        print(avg_df.count())

        db_conn = self.config.get_value('pg', 'conn')
        db_user = self.config.get_value('pg', 'user')
        db_password = self.config.get_value('pg', 'password')

        # raw_df.write.option("driver", "org.postgresql.Driver")\
        #  .option("user", db_user).option("password", db_password)\
        #  .jdbc(url=db_conn, table="f_klines", mode="append")

        spark.stop()

    def build_kpi(self, avg_df):
        ewma_50 = ti.ewma(50, avg_df)
        ewma_200 = ti.ewma(200, avg_df)
        ewma_250 = ti.ewma(250, avg_df)
        sma_50 = ti.simple_ma(50, avg_df)
        sma_200 = ti.simple_ma(200, avg_df)
        sma_250 = ti.simple_ma(250, avg_df)
        rsi_20 = ti.rsi(20, avg_df)
        rsi_50 = ti.rsi(50, avg_df)
        rsi_100 = ti.rsi(100, avg_df)
        fi_20 = ti.force_index(20, avg_df)
        fi_50 = ti.force_index(50, avg_df)
        fi_100 = ti.force_index(100, avg_df)
        kpi_dfs = [ewma_50, ewma_200, ewma_250, sma_50, sma_200, sma_250, rsi_20, rsi_50, rsi_100, fi_20, fi_50, fi_100]
        for kpi_df in kpi_dfs:
            avg_df = avg_df.union(kpi_df)
        return avg_df

    def prepare_avg_price(self, df: DataFrame) -> DataFrame:
        avg_df = df.withColumn("dvkpi_kpi_value", (col("close_price") + col("open_price"))/2)\
        .withColumn("dvkpi_kpi", lit("AVG_PRICE"))

        avg_df = avg_df.select("start_time",
                col("close_time").alias("dvkpi_timestamp"),
                "dvkpi_kpi",
                "dvkpi_kpi_value",
                "volume",
                col("symbol_id").alias("dvkpi_symbol_id"))

        return avg_df

t = Transform()
t.transform("ETHEUR")