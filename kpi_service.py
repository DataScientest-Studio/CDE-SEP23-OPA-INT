import pandas as pd

from cross_cutting.symbol_repository import SymbolRepository
from cross_cutting.utils import unix_to_datetime
import cross_cutting.technical_indicators as ti


def get_last_kpi_and_pivot(last_kline: object, symbol: object) -> object:
    sr = SymbolRepository()
    last_kpis = sr.get_last_kpis(symbol, last_kline)
    pivoted_df = last_kpis.pivot(index='dvkpi_timestamp_numeric', columns='dvkpi_kpi', values='dvkpi_kpi_value')
    pivoted_df.reset_index(inplace=True)
    pivoted_df.rename(columns={'dvkpi_timestamp_numeric': 'start_time_numeric'}, inplace=True)
    pivoted_df["start_time_numeric"] = pivoted_df["start_time_numeric"].astype(int)

    return pivoted_df


def fill_pivoted_kpis(last_binance_data: pd.DataFrame, symbol: str, last_kline: int,
                      pivoted_df: pd.DataFrame = None) -> pd.DataFrame:

    if pivoted_df is None:
        pivoted_df = get_last_kpi_and_pivot(last_kline, symbol)

    if not last_binance_data.empty:
        df_last_kline_necessary_data = last_binance_data[['start_time_numeric', 'open_price', 'close_price']]
        df_last_kline_necessary_data['open_price'] = df_last_kline_necessary_data['open_price'].astype(float)
        df_last_kline_necessary_data['close_price'] = df_last_kline_necessary_data['close_price'].astype(float)
        df_last_kline_necessary_data['AVG_PRICE'] = (df_last_kline_necessary_data['open_price'] +
                                                     df_last_kline_necessary_data['close_price']) / 2
        df_last_kline_necessary_data.drop(columns=['open_price', 'close_price'], inplace=True)
        pivoted_df = pd.concat([pivoted_df, df_last_kline_necessary_data])

    pivoted_df["date"] = pivoted_df["start_time_numeric"].apply(unix_to_datetime)

    pivoted_df = ti.simple_ma_pandas(50, pivoted_df)
    pivoted_df = ti.simple_ma_pandas(200, pivoted_df)
    pivoted_df = ti.simple_ma_pandas(250, pivoted_df)
    pivoted_df = ti.ewma_pandas(50, pivoted_df)
    pivoted_df = ti.ewma_pandas(200, pivoted_df)
    pivoted_df = ti.ewma_pandas(250, pivoted_df)
    pivoted_df = ti.rsi_pandas(20, pivoted_df)
    pivoted_df = ti.rsi_pandas(50, pivoted_df)
    pivoted_df = ti.rsi_pandas(100, pivoted_df)

    return pivoted_df