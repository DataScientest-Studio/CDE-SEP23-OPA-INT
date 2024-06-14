from datetime import datetime
import api_settings
import cross_cutting.db_driver as db_driver
import binance_recent_data
import cross_cutting.binance_response_formatter as bf

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
        df_res = binance_recent_data.handle_binance_recent_data(filename_output=None, api_key=api_key,
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