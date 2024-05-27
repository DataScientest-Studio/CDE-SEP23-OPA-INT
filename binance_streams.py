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


async def get_kline_data(bsm, api_key, api_secret, symbol, start_time):

    symbol_multi_socket = symbol.lower() + '@kline_1m'
    async with bsm.multiplex_socket([symbol_multi_socket]) as stream:
        while True:
            res = await stream.recv()
            res_dict = res["data"]["k"]
            res_dict_selected_keys = {key: res_dict[key] for key in keys_candles}
            res_df = pd.DataFrame(res_dict_selected_keys, index=pd.Series(res_dict["s"]))
            res_df = bf.fix_klines_dataset(res_df, 1)
            db_driver.insert_df_to_table(res_df, db_url, Settings.get_setting("klines_stream_table"))

            if time.time() - start_time > 10:
                print("Closing klines stream after 15 seconds")
                break
            print(res)


async def get_aggr_trade_data(bsm, api_key, api_secret, symbol, start_time):

    symbol_multi_socket = symbol.lower() + '@aggTrade'

    async with bsm.multiplex_socket([symbol_multi_socket]) as stream:
        while True:
            res = await stream.recv()
            res_dict = res["data"]
            res_dict_selected_keys = {key: res_dict[key] for key in key_agg_trades}
            res_df = pd.DataFrame(res_dict_selected_keys, index=pd.Series(res_dict["s"]))
            res_df = bf.fix_trades_dataset(res_df, 1)
            db_driver.insert_df_to_table(res_df, db_url, Settings.get_setting("aggregate_trades_stream_table"))

            if time.time() - start_time > 15:
                print("Closing aggr_trades stream after 15 seconds")
                break
            print(res)


async def main(api_key, api_secret, coin, fiat_curr, flag_use_demo_acc):
    client = await AsyncClient.create(api_key, api_secret, testnet=flag_use_demo_acc)
    bsm = BinanceSocketManager(client, user_timeout=20)

    symbol_txt = coin + fiat_curr
    start_time = time.time()
    await asyncio.gather(get_kline_data(bsm, api_key, api_secret, symbol_txt, start_time),
                         get_aggr_trade_data(bsm, api_key, api_secret, symbol_txt, start_time))
    await asyncio.sleep(10)
    await client.close_connection()


def run_main(api_key, api_secret, coin="ETH", fiat_curr="EUR", flag_use_demo_acc=True):
    return asyncio.run(main(api_key, api_secret, coin, fiat_curr, flag_use_demo_acc))


# if __name__ == "__main__":
# asyncio.run(main())
