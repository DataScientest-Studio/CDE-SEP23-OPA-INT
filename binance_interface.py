import pandas as pd
from binance.client import Client
from settings import Settings

class BinanceClient:
    def __init__(self):
        self.client = Client(Settings.get_setting('api_key_demo'), Settings.get_setting('api_key_demo'))

    def get_historical_klines(self, symbol: str) -> pd.DataFrame:
        resulting_df = pd.DataFrame(
            self.client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=1000))
        resulting_df['symbol'] = symbol
        return resulting_df

    def get_historical_trades(self, symbol: str) -> pd.DataFrame:
        resulting_df = pd.DataFrame(self.client.get_historical_trades(symbol=symbol, limit=1000))
        resulting_df['symbol'] = symbol
        return resulting_df


