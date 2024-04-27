import numpy as np
import pandas
import pyspark.sql.dataframe
import pyspark.pandas as ps
from pyspark.sql.functions import avg, col, lit
from pyspark.sql.window import Window



def simple_ma_pandas(n_periods: int, avg_df: pandas.DataFrame) -> pandas.DataFrame:
    col_name = f"SMA_{n_periods}"
    neu = avg_df['AVG_PRICE'].rolling(window=n_periods).mean()
    avg_df[col_name].iloc[n_periods:] = neu[n_periods:]
    return avg_df

def ewma_pandas(n_periods: int, avg_df: pandas.DataFrame) -> pandas.DataFrame:
    col_name = f"EWMA_{n_periods}"
    ewma = avg_df['AVG_PRICE'].ewm(span=n_periods, min_periods=n_periods).mean()
    avg_df[col_name].iloc[n_periods:] = ewma.iloc[n_periods:]
    return avg_df

def rsi_pandas(n_periods: int, avg_df: pandas.DataFrame) -> pandas.DataFrame:
    col_name = f"RSI_{n_periods}"
    avg_df['AVG_PRICE'] = avg_df['AVG_PRICE'].astype(float)
    # Calculate the difference in price
    delta = avg_df['AVG_PRICE'].diff()
    # Get the positive and negative gains
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    # Calculate the exponential moving average of gains and losses
    ema_gain = gain.ewm(span=n_periods, min_periods=n_periods).mean()
    ema_loss = loss.ewm(span=n_periods, min_periods=n_periods).mean()
    # Calculate the relative strength
    rs = ema_gain / ema_loss
    # Calculate the RSI
    rsi = 100 - (100 / (1 + rs))
    # Update the values of the specified column starting from the nth row
    avg_df[col_name].iloc[n_periods:] = rsi.iloc[n_periods:]
    return avg_df

def simple_ma(n_periods: int, avg_df: pyspark.sql.dataframe) -> pyspark.sql.dataframe:
    windowSpec = Window.orderBy(col("dvkpi_timestamp")).rowsBetween(-n_periods, 0)
    df = (avg_df.drop('dvkpi_kpi'). \
          withColumn("dvkpi_kpi", lit(f"SMA_{n_periods}")). \
          withColumn("moving_avg", avg(col("dvkpi_kpi_value")).over(windowSpec)). \
          drop("dvkpi_kpi_value").withColumnRenamed("moving_avg", "dvkpi_kpi_value"))

    return df


def avg_price(df: pyspark.sql.dataframe) -> pyspark.sql.dataframe:
    avg_df = df.withColumn("dvkpi_kpi_value", (col("close_price") + col("open_price")) / 2) \
        .withColumn("dvkpi_kpi", lit("AVG_PRICE"))

    avg_df = avg_df.select(col("start_time").alias("dvkpi_timestamp"),
                           col("start_time_numeric").alias("dvkpi_timestamp_numeric"),
                           "dvkpi_kpi",
                           "dvkpi_kpi_value",
                           "volume",
                           col("symbol_id").alias("dvkpi_symbol_id"))


    return avg_df

def ewma(n_periods: int, avg_df: pyspark.sql.dataframe) -> pyspark.sql.dataframe:
    ps.options.compute.ops_on_diff_frames = True
    psdf = avg_df.pandas_api()

    psdf['dvkpi_kpi_value'] = psdf['dvkpi_kpi_value'].ewm(span=n_periods, min_periods=n_periods - 1).mean()
    psdf['dvkpi_kpi'] = f"EWMA_{n_periods}"

    spark_df = psdf.to_spark()
    ps.options.compute.ops_on_diff_frames = False


    return spark_df

def rsi(n_periods: int, avg_df: pyspark.sql.dataframe) -> pyspark.sql.dataframe:
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

    psdf["dvkpi_kpi"] = f"RSI_{n_periods}"
    psdf["dvkpi_kpi_value"] = rsi_s
    psdf["dvkpi_kpi_value"] = psdf["dvkpi_kpi_value"].replace(np.nan, None)

    spark_df = psdf.to_spark()

    ps.options.compute.ops_on_diff_frames = False
    return spark_df




