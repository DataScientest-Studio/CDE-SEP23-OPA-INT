from cross_cutting import db_driver
from cross_cutting import technical_indicators as ti
import pandas as pd
import api_settings as settings


def create_derived_kpis(df_klines=None, symbol_id=1, create_from_predictions=False):
    db_url = settings.db_conn
    if df_klines is not None and create_from_predictions == False:
        df_klines.sort_values(by='start_time', ascending=True, inplace=True)

    if create_from_predictions == False:
        result_avg_klines = db_driver.create_derived_kpis(db_url, symbol_id, df_klines)
    else:
        result_avg_klines = db_driver.create_derived_kpis_from_pred(df_klines, symbol_id)

    result_avg_klines = result_avg_klines.drop_duplicates()
    result_avg_klines["dvkpi_timestamp"] = pd.to_datetime(result_avg_klines["dvkpi_timestamp"],
                                                          format='%Y-%m-%d %H:%M:%S', utc=True)
    result_avg_klines.sort_values(by='dvkpi_timestamp', ascending=True, inplace=True)

    ewma_50 = ti.ewma(db_url, "ETHEUR", 50, result_avg_klines)  # symbol_id = 1
    ewma_200 = ti.ewma(db_url, "ETHEUR", 200, result_avg_klines)  # symbol_id = 1
    ewma_250 = ti.ewma(db_url, "ETHEUR", 250, result_avg_klines)  # symbol_id = 1

    sma_50 = ti.simple_ma(db_url, "ETHEUR", 50, result_avg_klines)  # symbol_id = 1
    sma_200 = ti.simple_ma(db_url, "ETHEUR", 200, result_avg_klines)  # symbol_id = 1
    sma_250 = ti.simple_ma(db_url, "ETHEUR", 250, result_avg_klines)  # symbol_id = 1

    rsi_20 = ti.rsi(db_url, "ETHEUR", 20, result_avg_klines)
    rsi_50 = ti.rsi(db_url, "ETHEUR", 50, result_avg_klines)
    rsi_100 = ti.rsi(db_url, "ETHEUR", 100, result_avg_klines)

    results_all_kpis = pd.concat([result_avg_klines, ewma_50, ewma_200, ewma_250,
                                  sma_50, sma_200, sma_250, rsi_20, rsi_50, rsi_100], axis=0)

    if df_klines is None:
        results_all_kpis = db_driver.filter_derived_kpis(db_url, symbol_id, results_all_kpis)
        db_driver.insert_df_to_table(results_all_kpis, db_url, settings.kpi_table)
        return None

    elif create_from_predictions:
        return results_all_kpis
    else:
        db_driver.delete_data_from_table(settings.db_conn,
                                         settings.kpi_stream_table,
                                         symbol_id)
        db_driver.insert_df_to_table(results_all_kpis, db_url, settings.kpi_stream_table)
        return results_all_kpis
