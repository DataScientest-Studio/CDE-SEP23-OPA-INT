import asyncio
import os, time
import pandas as pd
import binance_response_formatter as bf
import db_driver
from settings import Settings
from binance import AsyncClient, BinanceSocketManager, Client


os.environ['TZ'] = 'UTC' # set timezone to UTC

# Define which keys to keep from streams
keys_candles = ["t","o", "h", "l", "c", "v", "T", "q", "n", "B", "Q", "s"]
key_agg_trades = ["a", "p", "q", "f", "l", "T", "m", "M"]
colnames_agg_trades = ["AggTradeID", "Price", "Quantity", "FirstTradeID", "LastTradeID", "Timestamp"]
db_url = Settings.get_setting("db_conn")

def ask_exit(signame, loop):
    print("got signal %s: exit" % signame)
    loop.stop()


async def get_kline_data(bsm, api_key, api_secret, symbol):

    symbol_multi_socket = symbol.lower() + '@kline_1m'
    start_time = time.time()
    async with bsm.multiplex_socket([symbol_multi_socket]) as stream:
        while True:
            res = await stream.recv()
            res_dict = res["data"]["k"]
            res_dict_selected_keys = {key: res_dict[key] for key in keys_candles}
            res_df = pd.DataFrame(res_dict_selected_keys, index=pd.Series(res_dict["s"]))
            res_df = bf.fix_klines_dataset(res_df, 1)
            db_driver.insert_df_to_table(res_df, db_url, Settings.get_setting("klines_stream_table"))

            if time.time() - start_time > 10:
                print("Closing stream after 10 seconds")
                break
            print(res_df)


async def get_aggr_trade_data(bsm, api_key, api_secret, symbol):
    try:
        file = open("aggr_trades.txt", "x")
    except:
        file = open("aggr_trades.txt", "w")
    bin_client = Client(api_key, api_secret)
    df_recent = pd.DataFrame(bin_client.get_aggregate_trades(symbol=symbol, limit=1000))
    df_recent = df_recent.iloc[:, 0:6]

    with open("aggr_trades.txt", "a") as file:
        file.write(df_recent.to_string(header=False, index=False))

    symbol_multi_socket = symbol.lower() + '@aggTrade'

    async with bsm.multiplex_socket([symbol_multi_socket]) as stream:
        start_time = time.time()
        while True:
            res = await stream.recv()
            res_dict = res["data"]
            res_dict_selected_keys = {key: res_dict[key] for key in key_agg_trades}
            res_df = pd.DataFrame(res_dict_selected_keys, index=pd.Series(res_dict["s"]))
            with open('aggr_trades.txt', 'a') as file:
                file.write(f'\n')
                file.write(res_df.to_string(header=False, index=False))
                file.close()
            if time.time() - start_time > 10:
                print("Closing stream after 10 seconds")
                break
            #print(res_df)


async def main(api_key, api_secret, coin, fiat_curr, flag_use_demo_acc):
    client = await AsyncClient.create(api_key, api_secret, testnet=flag_use_demo_acc)
    bsm = BinanceSocketManager(client, user_timeout=20)

    symbol_txt = coin + fiat_curr
    await asyncio.gather(get_kline_data(bsm, api_key, api_secret, symbol_txt),
                         get_aggr_trade_data(bsm, api_key, api_secret, symbol_txt))
    await asyncio.sleep(10)
    await client.close_connection()


def run_main(api_key, api_secret, coin="ETH", fiat_curr="EUR", flag_use_demo_acc=True):
    return asyncio.run(main(api_key, api_secret, coin, fiat_curr, flag_use_demo_acc))

# if __name__ == "__main__":
# asyncio.run(main())
