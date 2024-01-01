import pandas as pd
from binance import Client
import db_driver

def handle_binance_recent_data(filename_output, api_key, api_secret, symbol, range_start_cols, range_end_cols, db_conn,data_type):
    #queries
    data_return = query_binance(api_key, api_secret, symbol)
    #write_to_file
    write_data_to_file(filename_output, data_return, range_start_cols, range_end_cols)
    #write_on_db
    #todo add for all tables
    if data_type == 'klines':
        db_driver.insert_data_from_dataframe(data_return, db_conn, 'kline_data')

    return data_return;

def query_binance(api_key, api_secret, symbol):
    bin_client = Client(api_key, api_secret)
    resulting_df = pd.DataFrame(bin_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=1000))
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

