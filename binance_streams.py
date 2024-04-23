import asyncio
import os

import pandas as pd
from binance import AsyncClient, BinanceSocketManager

from binance_recent_data import BinanceRecentData
from cross_cutting.utils import unix_to_datetime

from cross_cutting.symbol_repository import SymbolRepository
from kpi_service import get_last_kpi_and_pivot, fill_pivoted_kpis
from settings import Settings

os.environ['TZ'] = 'UTC'  # set timezone to UTC

# Define which keys to keep from streams
keys_candles = ["t", "o", "h", "l", "c", "v", "T", "q", "n", "B", "Q", "s"]
db_url = Settings.get_setting("db_conn")


def handle_kline_stream_data(stream_dic: dict) -> pd.DataFrame:
    df = pd.DataFrame({
        'open_price': [float(stream_dic['o'])],
        'high_price': [float(stream_dic['h'])],
        'low_price': [float(stream_dic['l'])],
        'close_price': [float(stream_dic['c'])],
        'volume': [float(stream_dic['v'])],
        'number_of_trades': [stream_dic['n']],
        'close_time_numeric': [stream_dic['T']],
        'start_time_numeric': [stream_dic['t']],
        'symbol_id': 1  # TODO get symbol_id from symbol
    })

    df['start_time'] = df['start_time_numeric'].apply(unix_to_datetime)
    df['close_time'] = df['close_time_numeric'].apply(unix_to_datetime)

    return df


async def run_kline_stream(bsm, symbol):
    brd = BinanceRecentData()
    brd.handle_and_store_data(symbol)

    sr = SymbolRepository()
    last_kline = sr.get_last_kline_timestamp(symbol)

    kpis = get_last_kpi_and_pivot(last_kline, symbol)
    symbol_multi_socket = symbol.lower() + '@kline_1m'

    async with bsm.multiplex_socket([symbol_multi_socket]) as stream:
        while True:
            try:
                res = await stream.recv()
            except Exception as e:
                print(f"Exception while receiving stream: {e}")
                continue

            res_dict = res["data"]["k"]

            if res_dict["x"] == False:
                print(f"Kline stream is not closed{res_dict['t']}")
                continue

            print(f"Kline stream is closed{res_dict['t']}")

            res_dict_selected_keys = {key: res_dict[key] for key in keys_candles}
            current_kline_df = handle_kline_stream_data(res_dict_selected_keys)
            kpis = brd.handle_and_store_data(symbol, res_dict['t'], current_kline_df, kpis)


async def main(api_key, api_secret, coin, fiat_curr, flag_use_demo_acc):
    client = await AsyncClient.create(api_key, api_secret, testnet=flag_use_demo_acc)
    bsm = BinanceSocketManager(client, user_timeout=20)

    symbol_txt = coin + fiat_curr
    await asyncio.gather(run_kline_stream(bsm, symbol_txt))
    await asyncio.sleep(10)
    await client.close_connection()


def run_main(api_key, api_secret, coin="ETH", fiat_curr="EUR", flag_use_demo_acc=True):
    return asyncio.run(main(api_key, api_secret, coin, fiat_curr, flag_use_demo_acc))


if __name__ == "__main__":
    asyncio.run(main(api_key=Settings.get_setting("api_key_demo"), api_secret=Settings.get_setting("api_secret_demo"),
                     coin="ETH", fiat_curr="EUR", flag_use_demo_acc=True))
