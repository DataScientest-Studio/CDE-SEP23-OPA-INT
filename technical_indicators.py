from sqlalchemy import create_engine, Table, MetaData, select
import pandas as pd
import numpy as np
import db_driver as dbd

def ewma(db_url, symbol_id, n_periods, df_klines=None):

    engine = create_engine(db_url)
    with engine.connect() as connection:
        try:
            if df_klines is None:
                avg_prices = Table("f_dvkpi", MetaData(), autoload_with=engine)

                query = select(avg_prices).where((avg_prices.c.dvkpi_kpi == 'AVG_PRICE'
                                                  and avg_prices.c.symbol_id == symbol_id))
                # Execute the query and fetch the results
                result = connection.execute(query)
            else:
                result = df_klines.copy(deep=True)

            result_df = pd.DataFrame(result)
            result_df["dvkpi_kpi"] = "EWMA_" + str(n_periods)
            ewma = pd.Series(result_df["dvkpi_kpi_value"].ewm(span = n_periods, min_periods = n_periods-1).mean(), name ='EWMA_' + str(n_periods))
            result_df = pd.concat([result_df[["dvkpi_timestamp", "dvkpi_symbol_id", "dvkpi_kpi"]],ewma], axis=1)
            result_df.columns = ["dvkpi_timestamp", "dvkpi_symbol_id", "dvkpi_kpi", "dvkpi_kpi_value"]
            result_df["dvkpi_kpi_value"] = result_df["dvkpi_kpi_value"].replace(np.nan,None)
            if df_klines is None:
                dbd.insert_df_to_table(result_df, db_url, "f_dvkpi")

        except Exception as e:
            print(f"Error calculating exponential moving averages: {e}")
    return result_df

def simple_ma(db_url, symbol_id, n_periods, df_klines=None):
    engine = create_engine(db_url)
    with engine.connect() as connection:
        try:
            if df_klines is None:
                avg_prices = Table("f_dvkpi", MetaData(), autoload_with=engine)

                query = select(avg_prices).where((avg_prices.c.dvkpi_kpi == 'AVG_PRICE'
                                                  and avg_prices.c.symbol_id == symbol_id))
                # Execute the query and fetch the results
                result = connection.execute(query)
            else:
                result = df_klines.copy(deep=True)

            result_df = pd.DataFrame(result)
            result_df["dvkpi_kpi"] = "MA_" + str(n_periods)
            sma = pd.Series(result_df["dvkpi_kpi_value"].rolling(n_periods).mean(),
                             name="MA_" + str(n_periods))
            result_df = pd.concat([result_df[["dvkpi_timestamp", "dvkpi_symbol_id", "dvkpi_kpi"]], sma], axis=1)
            result_df.columns = ["dvkpi_timestamp", "dvkpi_symbol_id", "dvkpi_kpi", "dvkpi_kpi_value"]
            result_df["dvkpi_kpi_value"] = result_df["dvkpi_kpi_value"].replace(np.nan,None)
            if df_klines is None:
                dbd.insert_df_to_table(result_df, db_url, "f_dvkpi")

        except Exception as e:
            print(f"Error calculating simple moving averages: {e}")
    return result_df

def rsi(db_url, symbol_id, periods=14, df_klines=None):

    engine = create_engine(db_url)
    with engine.connect() as connection:
        try:
            if df_klines is None:
                avg_prices = Table("f_dvkpi", MetaData(), autoload_with=engine)

                query = select(avg_prices).where((avg_prices.c.dvkpi_kpi == 'AVG_PRICE' and avg_prices.c.symbol_id ==
                                                  symbol_id))
                # Execute the query and fetch the results
                result = connection.execute(query)
            else:
                result = df_klines.copy(deep=True)

            result_df = pd.DataFrame(result)
            # filter out rows where price = None
            result_df = result_df[result_df[['dvkpi_kpi_value']].notnull().all(1)]

            close_delta = result_df['dvkpi_kpi_value'].diff()
            # Make two series: one for lower closes and one for higher closes
            up = close_delta.clip(lower=0)
            down = -1 * close_delta.clip(upper=0)

            ma_up = up.ewm(com=periods - 1, adjust=True, min_periods=periods).mean()
            ma_down = down.ewm(com=periods - 1, adjust=True, min_periods=periods).mean()

            rsi_s = ma_up / ma_down
            rsi_s = 100 - (100/(1 + rsi_s))

            result_df["dvkpi_kpi"] = "RSI_" + str(periods)
            result_df = pd.concat([result_df[["dvkpi_timestamp", "dvkpi_symbol_id", "dvkpi_kpi"]], rsi_s], axis=1)
            result_df.columns = ["dvkpi_timestamp", "dvkpi_symbol_id", "dvkpi_kpi", "dvkpi_kpi_value"]
            result_df["dvkpi_kpi_value"] = result_df["dvkpi_kpi_value"].replace(np.nan,None)
            if df_klines is None:
                dbd.insert_df_to_table(result_df, db_url, "f_dvkpi")

        except Exception as e:
            print(f"Error calculating RSI: {e}")
    return result_df