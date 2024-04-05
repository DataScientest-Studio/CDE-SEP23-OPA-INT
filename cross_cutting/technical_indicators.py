import numpy as np
import pyspark.sql.dataframe
import pyspark.pandas as ps
from pyspark.sql.functions import avg, col, lit, lag
from pyspark.sql.window import Window


def ewma(n_periods: int, avg_df: pyspark.sql.dataframe):
    ps.options.compute.ops_on_diff_frames = True
    psdf = avg_df.pandas_api()

    psdf['dvkpi_kpi_value'] = psdf['dvkpi_kpi_value'].ewm(span=n_periods, min_periods=n_periods - 1).mean()
    psdf['dvkpi_kpi'] = f"EWMA_{n_periods}"

    spark_df = psdf.to_spark()
    ps.options.compute.ops_on_diff_frames = False

    return spark_df


def simple_ma(n_periods: int, avg_df: pyspark.sql.dataframe):
    windowSpec = Window.orderBy(col("dvkpi_timestamp")).rowsBetween(-n_periods, 0)
    df = avg_df.drop('dvkpi_kpi').withColumn("dvkpi_kpi", lit(f"SMA_{n_periods}")). \
        withColumn("moving_avg", avg(col("dvkpi_kpi_value")).over(windowSpec)). \
        drop("dvkpi_kpi_value").withColumnRenamed("moving_avg", "dvkpi_kpi_value")

    return df


def rsi(n_periods: int, avg_df: pyspark.sql.dataframe):
    ps.options.compute.ops_on_diff_frames = True
    psdf = avg_df.pandas_api()

    close_delta = psdf['dvkpi_kpi_value'].diff()
    # Make two series: one for lower closes and one for higher closes
    up = close_delta.clip(lower=0)
    down = -1 * close_delta.clip(upper=0)

    ma_up = up.ewm(com=n_periods - 1, min_periods=n_periods).mean()
    ma_down = down.ewm(com=n_periods - 1, min_periods=n_periods).mean()

    rsi_s = ma_up / ma_down
    rsi_s = 100 - (100 / (1 + rsi_s))

    psdf["dvkpi_kpi"] = "RSI_" + str(n_periods)
    psdf["dvkpi_kpi_value"] = rsi_s
    psdf["dvkpi_kpi_value"] = psdf["dvkpi_kpi_value"].replace(np.nan, None)

    spark_df = psdf.to_spark()
    ps.options.compute.ops_on_diff_frames = False
    return spark_df


def force_index(n_periods: int, avg_df: pyspark.sql.dataframe):
    windowSpec = Window.orderBy(col("dvkpi_timestamp"))
    df = avg_df.withColumn("force_index", (col("dvkpi_kpi_value") - lag("dvkpi_kpi_value", n_periods).over(windowSpec)) * col("volume")). \
        drop('dvkpi_kpi').withColumn("dvkpi_kpi", lit(f"FI{n_periods}")). \
        drop("dvkpi_kpi_value").withColumnRenamed("force_index", "dvkpi_kpi_value")

    return df

