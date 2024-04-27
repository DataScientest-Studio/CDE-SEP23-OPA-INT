import numpy as np
import pandas
import pyspark.sql.dataframe
import pyspark.pandas as ps
from pyspark.sql.functions import avg, col, lit, lag
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






