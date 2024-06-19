import pandas as pd
from binance import Client
import time
import datetime
import api_settings as api_settings
import db_driver as db_driver
import binance_response_formatter as bf

def handle_binance_recent_data(filename_output, api_key, api_secret, symbol,data_type,
                               timespan_av_min, timespan_av_max, ts_start_date_numeric,
                               seconds_shift):

    ts_end_date_numeric = round(time.time_ns() / 1000000)
    #create empty dataframe
    data_return = pd.DataFrame({})
    ts_latest_value = ts_start_date_numeric

    # explanation range: numeric values are in milliseconds. We have a limit of 1000.
    # Since we want 1m intervals the step must be 1000 * 60 *1000

    #TODO include try and except in case that query yields error
    while ts_latest_value < ts_end_date_numeric-1000*seconds_shift:
        if not (ts_latest_value >= timespan_av_min and ts_latest_value <= timespan_av_max):
            print("loading for", pd.to_datetime(ts_latest_value, unit='ms', utc=True), "UTC onwards")
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
        resulting_df = pd.DataFrame(bin_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE,
                                                          limit=1000, startTime=t))
    elif data_type == "aggr_trades":
        resulting_df = pd.DataFrame(
            bin_client.get_aggregate_trades(symbol=symbol,limit=1000, startTime=t))
    else:
        print("Data Type not found")
    return resulting_df

def load_recent_data(api_key, api_sec, symbol_id=1):
    # get main parameters
    symbol_txt = f"{api_settings.coin_to_trade}{api_settings.fiat_curr}"

    # Get Recent Data
    l_data_type = ["klines"]

    dict_frames = {}
    min_date_numeric = datetime.combine(datetime.date(2020, 1, 1), datetime.min.time()).timestamp() * 1000
    ts_start_date_numeric = min_date_numeric

    # maximum timestamp available in Database
    # Bot needs at least 4 hours of data due to MA calculations
    # TODO: if it was called early in the morning adjust max_date_numeric
    max_date_numeric = datetime.combine(datetime.date.today(), datetime.min.time()).timestamp() * 1000

    for data_type in l_data_type:
        print(f"Fetching data on {data_type} for today, please wait")
        df_res = handle_binance_recent_data(filename_output=None, api_key=api_key,
                                                                api_secret=api_sec, symbol=symbol_txt,
                                                                data_type=data_type,
                                                                timespan_av_min=int(min_date_numeric),
                                                                timespan_av_max=int(max_date_numeric),
                                                                ts_start_date_numeric=int(ts_start_date_numeric),
                                                                seconds_shift=30)

        if data_type == "klines":
            # First delete data from aggregates stream table, stream table is only temporary data
            db_driver.delete_data_from_table(api_settings.db_conn,
                                             api_settings.klines_stream_table,
                                             symbol_id)
            df_res = bf.fix_klines_dataset(df_res, symbol_id)
            # import data into stream data
            db_driver.insert_df_to_table(df_res, api_settings.db_conn,
                                         api_settings.klines_stream_table)
        else:
            break
        dict_frames[data_type] = df_res

    return dict_frames

