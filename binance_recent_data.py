from binance import Client
import pandas as pd
def get_recent_data(filename_output, api_key, api_secret, symbol, data_type,range_start_cols,range_end_cols):
    try:
        file = open("kline_data.txt", "x")
    except:
        file = open("kline_data.txt", "w")
    bin_client = Client(api_key, api_secret)
    df_recent = pd.DataFrame(bin_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=1000))
    df_recent = df_recent.iloc[:, range_start_cols:range_end_cols]

    with open("kline_data.txt", "a") as file:
        file.write(df_recent.to_string(header=False, index=False))
    return df_recent