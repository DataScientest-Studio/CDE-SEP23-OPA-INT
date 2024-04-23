from datetime import datetime, timezone

import pandas as pd
from binance import Client
import settings
import binance_response_formatter as brf
import db_driver as db
from cross_cutting.symbol_repository import SymbolRepository
from kpi_service import fill_pivoted_kpis


class BinanceRecentData:
    def __init__(self):
        api_key = settings.Settings.get_setting("api_key_demo")
        api_secret = settings.Settings.get_setting("api_key_secret_demo")
        self.Client = Client(api_key, api_secret)

    def query_binance(self, symbol: str, t: int) -> pd.DataFrame:
        try:
            column_names = ['start_time_numeric', 'open_price', 'high_price', 'low_price',
                            'close_price', 'volume', 'close_time_numeric', 'quote_asset_volume',
                            'number_of_trades', 'taker_buy_base_asset_volume',
                            'taker_buy_quote_asset_volume', 'ignore']

            resulting_df = pd.DataFrame(self.Client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE,
                                                               limit=1000, startTime=t), columns=column_names)

            last_time = resulting_df['start_time_numeric'].iloc[-1]

            utc_now = datetime.now(timezone.utc)
            utc_timestamp_ms = int(utc_now.timestamp() * 1000)

            utc_60_seconds_ago = utc_timestamp_ms - 60000
            while last_time <= utc_60_seconds_ago:
                df_2 = pd.DataFrame(self.Client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE,
                                                           limit=1000, startTime=last_time),
                                    columns=column_names)
                resulting_df = resulting_df.append(df_2)
                last_time = resulting_df['start_time_numeric'].iloc[-1]


        except Exception as e:
            print(f"Error while querying binance: {e}")
            raise

        resulting_df = resulting_df.drop(['taker_buy_base_asset_volume',
                                          'taker_buy_quote_asset_volume', 'ignore', 'quote_asset_volume'], axis=1)

        resulting_df.drop_duplicates(subset=['start_time_numeric'], keep='last', inplace=True)
        return resulting_df.tail(-1)

    def handle_and_store_data(self, symbol: str, last_kline: int = 0, last_binance: pd.DataFrame = None,
                                    pivoted_kpi: pd.DataFrame = None) -> pd.DataFrame:
        sr = SymbolRepository()
        binance_recent_data_client = BinanceRecentData()

        if last_kline == 0:
            last_kline = sr.get_last_kline_timestamp(symbol)

        if last_binance is None:
            last_binance = binance_recent_data_client.query_binance(symbol, last_kline)

        kpis = fill_pivoted_kpis(last_binance, symbol, last_kline, pivoted_kpi)

        filtered_kpis = kpis[kpis['start_time_numeric'] >= last_binance['start_time_numeric'].iloc[0]]
        melted = filtered_kpis.melt(id_vars=['start_time_numeric', 'date'], var_name='dv_kpi',
                                    value_name='dv_kpi_value')
        melted = melted.drop_duplicates(subset=['start_time_numeric', 'dv_kpi'], keep='last')

        try:
            binance_recent_data_client.insert_new_data(sr.get_symbol_id(symbol), melted, last_binance)
            return kpis
        except Exception as e:
            raise e

    def insert_new_data(self, symbol: int, new_kpi: pd.DataFrame, new_kline: pd.DataFrame):
        if not new_kline.empty and not new_kpi.empty:
            df_kpi = brf.fix_kpis_dataset(new_kpi, symbol)
            df_kline = brf.fix_klines_dataset(new_kline, symbol)

            db.insert_df_to_multiple_tables([df_kline, df_kpi], ['f_klines', 'f_dvkpi'])
