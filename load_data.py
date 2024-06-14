from src.trading_bot_api.cross_cutting import Settings, technical_indicators as ti
from sqlalchemy import create_engine, Table, MetaData, select
from datetime import datetime, date
import pandas as pd
from src.trading_bot_api.cross_cutting import binance_response_formatter as bf, db_driver
from src.trading_bot_api import binance_recent_data
import json


# Startup DB config

def create_db():
    db_url = Settings.get_setting("db_conn")
    # run it for symbols first since foreign keys depend on it
    db_driver.create_db(db_url, True)
    db_driver.create_db(db_url, False)


def load_settings(settings_file_name):
    with open(settings_file_name, "r") as settings_file:
        return json.load(settings_file)


# Function loads historical data, either from CSV and/or through looping API
def load_historical_data(api_key, api_sec, symbol_id=1):
    # get main parameters
    symbol_txt = Settings.get_setting["coin_to_trade"] + Settings.get_setting["fiat_curr"]
    load_from_csv = Settings.get_setting["load_from_csv"]
    # First date across time series, from this date onwards data is fetched
    ts_start_date_numeric = Settings.get_setting("time_series_start_numeric")

    dict_frames = {}
    l_data_type = ["klines", "aggr_trades"]
    settings = load_settings("settings.json")

    data_type = "aggr_trades"
    filename_output = (settings["tables"][data_type]["file_path_hist"] +
                       settings["tables"][data_type]["filename_output"])

    # Check Data availability in database
    min_date_numeric, max_date_numeric = get_min_and_max_dates(Settings.get_setting("db_conn"),
                                                               Settings.get_setting('aggregate_trades_table'))

    if load_from_csv == "True":
        print("Loading data from csv for aggregate trades")
        # TODO extend this method to read from multiple files
        try:
            print("local dataset found")
            df_aggregates = pd.read_csv('historical_data/aggtrades/aggtrades.csv', header=None)
            min_date_numeric = min(min_date_numeric, df_aggregates.iloc[:, 5].min())
            max_date_numeric = max(max_date_numeric, df_aggregates.iloc[:, 5].max())
        except:
            print("local dataset not found, use backup instead")
            df_aggregates = pd.read_csv('historical_data/aggtrades/aggtrades_rubem.csv', header=None)

    else:
        df_aggregates = pd.DataFrame({})

    print("Fetching recent/ missing data for aggregate trades")
    df_res = binance_recent_data.handle_binance_recent_data(filename_output=filename_output, api_key=api_key,
                                                            api_secret=api_sec, symbol=symbol_txt,
                                                            data_type=data_type,
                                                            timespan_av_min=min_date_numeric,
                                                            timespan_av_max=max_date_numeric,
                                                            ts_start_date_numeric=ts_start_date_numeric,
                                                            seconds_shift=60)
    # Concat and Cleanse data
    l_colnames = list(df_res.columns)
    df_aggregates.columns = l_colnames
    df_aggregates = pd.concat([df_aggregates, df_res], axis=0)
    df_aggregates = bf.fix_trades_dataset(df_aggregates, symbol_id)

    # TODO develop function which checks for completeness
    # Insert concatenated dataframe into db
    db_driver.insert_df_to_table(df_aggregates, Settings.get_setting("db_conn"),
                                 Settings.get_setting('aggregate_trades_table'))

    data_type = "klines"
    filename_output = (settings["tables"][data_type]["file_path_hist"] +
                       settings["tables"][data_type]["filename_output"])

    if load_from_csv == "True":
        print("Loading data from csv for klines")
        try:
            df_klines = pd.read_csv('historical_data/klines/klines.csv', header=None)
            min_date_numeric = min(min_date_numeric, df_klines.iloc[:, 0].min())
            max_date_numeric = max(max_date_numeric, df_klines.iloc[:, 0].max())
            print("local dataset found")
        except:
            print("local dataset not found")
            df_klines = pd.DataFrame({})
    else:
        df_klines = pd.DataFrame({})

    print("Fetching recent/ missing data for klines")
    df_res = binance_recent_data.handle_binance_recent_data(filename_output=filename_output, api_key=api_key,
                                                            api_secret=api_sec, symbol=symbol_txt,
                                                            data_type="klines",
                                                            timespan_av_min=min_date_numeric,
                                                            timespan_av_max=max_date_numeric,
                                                            ts_start_date_numeric=ts_start_date_numeric,
                                                            seconds_shift=60)
    l_colnames = list(df_res.columns)
    df_klines.columns = l_colnames
    df_klines = pd.concat([df_klines, df_res])
    df_klines = bf.fix_klines_dataset(df_klines, 1)
    db_driver.insert_df_to_table(df_klines, Settings.get_setting("db_conn"), Settings.get_setting('klines_table'))

    # Fetching Symbols Data
    df_symbol = pd.DataFrame({'symbol': 'ETHEUR'}, index=[0])
    db_driver.insert_df_to_table(df_symbol, Settings.get_setting("db_conn"), Settings.get_setting('symbols_table'))


def load_symbol_id(symbol):
    db_url = Settings.get_setting("db_conn")
    engine = create_engine(db_url)
    with engine.connect() as connection:
        try:
            symbols = Table("d_symbols", MetaData(), autoload_with=engine)
            stmt = select(symbols).where(symbols.c.symbol == symbol)
            result = connection.execute(stmt)
            symbol_id = pd.DataFrame(result).iloc[0, 0]
        except Exception as e:
            print(f"Error retrieving data: {e}")
    return symbol_id


def load_recent_data(api_key, api_sec, symbol_id=1):
    # get main parameters
    symbol_txt = Settings.get_setting("coin_to_trade") + Settings.get_setting("fiat_curr")

    # Get Recent Data
    l_data_type = ["klines"]

    dict_frames = {}
    min_date_numeric = datetime.combine(date(2020, 1, 1), datetime.min.time()).timestamp() * 1000
    ts_start_date_numeric = min_date_numeric

    # maximum timestamp available in Database
    # Bot needs at least 4 hours of data due to MA calculations
    # TODO: if it was called early in the morning adjust max_date_numeric
    max_date_numeric = datetime.combine(date.today(), datetime.min.time()).timestamp() * 1000

    for data_type in l_data_type:
        print(f"Fetching data on {data_type} for today, please wait")
        df_res = binance_recent_data.handle_binance_recent_data(filename_output=None, api_key=api_key,
                                                                api_secret=api_sec, symbol=symbol_txt,
                                                                data_type=data_type,
                                                                timespan_av_min=int(min_date_numeric),
                                                                timespan_av_max=int(max_date_numeric),
                                                                ts_start_date_numeric=int(ts_start_date_numeric),
                                                                seconds_shift=30)

        if data_type == "aggr_trades":
            # First delete data from aggregates stream table, stream table is only temporary data
            db_driver.delete_data_from_table(Settings.get_setting("db_conn"),
                                             Settings.get_setting('aggregate_trades_stream_table'),
                                             symbol_id)

            df_res = bf.fix_trades_dataset(df_res, symbol_id).drop_duplicates()
            # import data into stream data
            db_driver.insert_df_to_table(df_res, Settings.get_setting("db_conn"),
                                         Settings.get_setting('aggregate_trades_stream_table'))
        elif data_type == "klines":
            # First delete data from aggregates stream table, stream table is only temporary data
            db_driver.delete_data_from_table(Settings.get_setting("db_conn"),
                                             Settings.get_setting('klines_stream_table'),
                                             symbol_id)
            df_res = bf.fix_klines_dataset(df_res, symbol_id)
            # import data into stream data
            db_driver.insert_df_to_table(df_res, Settings.get_setting("db_conn"),
                                         Settings.get_setting('klines_stream_table'))
        else:
            break
        dict_frames[data_type] = df_res

    return dict_frames


def load_data_from_db_table(db_url, table_name, symbol_id=1):
    df = pd.DataFrame(db_driver.get_data_from_db_table(db_url, table_name, symbol_id))
    return df


# Function creates derived KPIs either database or from df_klines
# if df_klines is not None data is from the recent data stream (temporary data)
def create_derived_kpis(df_klines=None, symbol_id=1, create_from_predictions=False):
    db_url = Settings.get_setting("db_conn")
    symbol = Settings.get_setting("coin_to_trade") + Settings.get_setting("fiat_curr")
    if df_klines is not None and create_from_predictions == False:
        df_klines.sort_values(by='start_time', ascending=True, inplace=True)

    # When create_derived_kpis is called from ML prediction (future data), it has less columns
    if create_from_predictions == False:
        result_avg_klines = db_driver.create_derived_kpis(db_url, symbol_id, df_klines)
    else:
        result_avg_klines = db_driver.create_derived_kpis_from_pred(df_klines, symbol_id)

    # Remove duplicates and sort
    result_avg_klines = result_avg_klines.drop_duplicates()
    result_avg_klines["dvkpi_timestamp"] = pd.to_datetime(result_avg_klines["dvkpi_timestamp"], format='%Y-%m-%d %H:%M:%S', utc=True)
    result_avg_klines.sort_values(by='dvkpi_timestamp', ascending=True, inplace=True)

    # Exponentially weighted Moving Average
    ewma_50 = ti.ewma(db_url, "ETHEUR", 50, result_avg_klines)  # symbol_id = 1
    ewma_200 = ti.ewma(db_url, "ETHEUR", 200, result_avg_klines)  # symbol_id = 1
    ewma_250 = ti.ewma(db_url, "ETHEUR", 250, result_avg_klines)  # symbol_id = 1

    # Simple Moving average
    sma_50 = ti.simple_ma(db_url, "ETHEUR", 50, result_avg_klines)  # symbol_id = 1
    sma_200 = ti.simple_ma(db_url, "ETHEUR", 200, result_avg_klines)  # symbol_id = 1
    sma_250 = ti.simple_ma(db_url, "ETHEUR", 250, result_avg_klines)  # symbol_id = 1

    # RSI
    rsi_20 = ti.rsi(db_url, "ETHEUR", 20, result_avg_klines)
    rsi_50 = ti.rsi(db_url, "ETHEUR", 50, result_avg_klines)
    rsi_100 = ti.rsi(db_url, "ETHEUR", 100, result_avg_klines)

    # Concatenate all KPIs so that existing data is not fetched (otherwise primary key error)
    results_all_kpis = pd.concat([result_avg_klines, ewma_50, ewma_200, ewma_250,
                                  sma_50, sma_200, sma_250, rsi_20, rsi_50, rsi_100], axis=0)

    # if df_klines is None then function was called from historical data
    if df_klines is None:
        # Insert returned data into database
        results_all_kpis = db_driver.filter_derived_kpis(db_url, symbol_id, results_all_kpis)
        db_driver.insert_df_to_table(results_all_kpis, db_url, Settings.get_setting('kpi_table'))
        return None

    # if create_from_predictions is True then function was called from ML prediction, no storing in database
    elif create_from_predictions == True:
        return results_all_kpis
    # if df_klines is not None then function was called from recent data stream
    # insert to stream kpi table and return to memory
    else:
        db_driver.delete_data_from_table(Settings.get_setting("db_conn"),
                                         Settings.get_setting('kpi_stream_table'),
                                         symbol_id)
        db_driver.insert_df_to_table(results_all_kpis, db_url, Settings.get_setting('kpi_stream_table'))
        return results_all_kpis


def get_min_and_max_dates(db_url, table_name):
    engine = create_engine(db_url)
    min_date_numeric = None
    max_date_numeric = None

    with engine.connect() as connection:
        try:
            if table_name == "f_aggr_trades":
                result = connection.execute(f'SELECT MIN(tx_time_numeric), MAX(tx_time_numeric) FROM {table_name}')
            elif table_name == "f_klines":
                result = connection.execute(f'SELECT MIN(start_time), MAX(start_time) FROM {table_name}')
            else:
                raise Exception

            for elem in result:
                min_date_numeric = elem[0]
                max_date_numeric = elem[1]
        except Exception as e:
            print(f"Error retrieving data: {e}")

    return min_date_numeric, max_date_numeric
