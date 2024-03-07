import pandas as pd
from binance import Client
import db_driver
import time

def handle_binance_recent_data(filename_output, api_key, api_secret, symbol,data_type,
                               timespan_av_min, timespan_av_max, ts_start_date_numeric,
                               seconds_shift):

    ts_end_date_numeric = round(time.time_ns() / 1000000)
    #create empty dataframe
    data_return = pd.DataFrame({})
    ts_latest_value = ts_start_date_numeric

    # explanation range: numeric values are in milliseconds. We have a limit of 1000. Since we want 1m intervals the step must be 1000 * 60 *1000
    #TODO extend the loop for aggregate trades as theres no guarantee that the range is always homogenous
    #TODO include try and except in case that query yields error
    while ts_latest_value < ts_end_date_numeric-1000*seconds_shift:
        if not (ts_latest_value >= timespan_av_min and ts_latest_value <= timespan_av_max):
            print("loading for", pd.to_datetime(ts_latest_value, unit='ms', utc=True))
            data_res = query_binance(api_key, api_secret, symbol, data_type, ts_latest_value)
            if data_type == "klines":
                ts_latest_value = data_res.iloc[:,6].max()
            else:
                ts_latest_value = data_res.iloc[:,5].max()
            data_return = pd.concat([data_res, data_return])
        else:
            ts_latest_value = timespan_av_max + 1000

    # write or append to file
    if filename_output is not None:
        data_return.to_csv(filename_output, mode='a', header=False, index=False)

    return data_return

def query_binance(api_key, api_secret, symbol, data_type, t):
    bin_client = Client(api_key, api_secret)
    if data_type == "klines":
        resulting_df = pd.DataFrame(bin_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=1000, startTime=t))
    elif data_type == "aggr_trades":
        resulting_df = pd.DataFrame(
            bin_client.get_aggregate_trades(symbol=symbol,limit=1000, startTime=t))
    else:
        print("Data Type not found")
    return resulting_df

def write_data_to_file(filename_output, queried_df, range_start_cols, range_end_cols):
    try:
        file = open(filename_output, "x")
    except:
        file = open(filename_output, "w")
    df_recent_write = queried_df.iloc[:, range_start_cols:range_end_cols]

    with open(filename_output, "a") as file:
        file.write(df_recent_write.to_string(header=False, index=False))
    return df_recent_write

