import db_driver
from settings import Settings
from sqlalchemy import create_engine, Table, MetaData, select
from datetime import datetime, date
import pandas as pd
import binance_response_formatter as bf
import technical_indicators as ti
import binance_recent_data
import json
import time


#Startup DB config

def create_db():
    db_url = Settings.get_setting("db_conn")
    # run it for symbols first since foreign keys depend on it
    db_driver.create_db(db_url, True)
    db_driver.create_db(db_url, False)
def load_settings(settings_file_name):
    with open(settings_file_name, "r") as settings_file:
        return json.load(settings_file)

# Function loads historical data, either from CSV and/or through looping API
def load_historical_data(api_key, api_sec):

    # get main parameters
    symbol_txt = Settings.get_setting["coin_to_trade"] + Settings.get_setting["fiat_curr"]
    load_from_csv = Settings.get_setting["load_from_csv"]
    # First date across time series, from this date onwards data is fetched
    ts_start_date_numeric = Settings.get_setting("time_series_start_numeric")

    #TODO write function to get symbol_id for symbol
    df_symbol = pd.DataFrame({'symbol_id': 1, 'symbol': symbol_txt}, index=[0])
    db_driver.insert_df_to_table(df_symbol, Settings.get_setting("db_conn"), Settings.get_setting('symbols_table'))

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
        #TODO extend this method to read from multiple files
        try:
            print("local dataset found")
            df_aggregates = pd.read_csv('historical_data/aggtrades/aggtrades.csv', header=None)
            min_date_numeric = min(min_date_numeric, df_aggregates.iloc[:,5].min())
            max_date_numeric = max(max_date_numeric, df_aggregates.iloc[:,5].max())
        except:
            print("local dataset not found, use backup instead")
            df_aggregates = pd.read_csv('historical_data/aggtrades/aggtrades_rubem.csv', header=None)

    else:
        df_aggregates = pd.DataFrame({})

    print("Fetching recent/ missing data for aggregate trades")
    df_res = binance_recent_data.handle_binance_recent_data(filename_output=filename_output, api_key=api_key,
                                                            api_secret=api_sec, symbol=symbol_txt,
                                                            data_type=data_type,
                                                            timespan_av_min = min_date_numeric,
                                                            timespan_av_max = max_date_numeric,
                                                            ts_start_date_numeric = ts_start_date_numeric,
                                                            seconds_shift=60)
    # Concat and Cleanse data
    l_colnames = list(df_res.columns)
    df_aggregates.columns = l_colnames
    df_aggregates = pd.concat([df_aggregates,df_res], axis=0)
    df_aggregates = bf.fix_trades_dataset(df_aggregates, 1)

    #df_aggregates = pd.concat([df_aggregates.iloc[80000000:len(df_aggregates),:],df_res], axis=0)
    #df_aggregates=df_aggregates.drop_duplicates()
    #df_aggregates.to_csv("aggrtrades_pg4.csv", mode='a', header=False, index=False)
    # TODO develop function which checks for completeness

    #Insert concatenated dataframe into db
    db_driver.insert_df_to_table(df_aggregates, Settings.get_setting("db_conn"), Settings.get_setting('aggregate_trades_table'))


    data_type = "klines"
    filename_output = (settings["tables"][data_type]["file_path_hist"] +
                       settings["tables"][data_type]["filename_output"])

    # Check Data availability in database
    min_date_str, max_date_str = get_min_and_max_dates(Settings.get_setting("db_conn"),
                                                               Settings.get_setting('klines_table'))

    if load_from_csv == "True":
        print("Loading data from csv for klines")
        try:
            df_klines = pd.read_csv('historical_data/klines/klines.csv', header=None)
            min_date_numeric = min(min_date_numeric, df_klines.iloc[:,0].min())
            max_date_numeric = max(max_date_numeric, df_klines.iloc[:,0].max())
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
                                                            timespan_av_min = min_date_numeric,
                                                            timespan_av_max = max_date_numeric,
                                                            ts_start_date_numeric = ts_start_date_numeric,
                                                            seconds_shift=60)
    l_colnames = list(df_res.columns)
    df_klines.columns = l_colnames
    df_klines = pd.concat([df_klines,df_res])
    df_klines = bf.fix_klines_dataset(df_klines, 1)
    db_driver.insert_df_to_table(df_klines, Settings.get_setting("db_conn"), Settings.get_setting('klines_table'))

    # Fetching Symbols Data
    df_symbol = pd.DataFrame({'symbol': 'ETHEUR'}, index=[0])
    db_driver.insert_df_to_table(df_symbol, Settings.get_setting("db_conn"), Settings.get_setting('symbols_table'))

def load_recent_data(api_key, api_sec):
    # get main parameters
    symbol_txt = Settings.get_setting("coin_to_trade") + Settings.get_setting("fiat_curr")

    # Get Recent Data
    l_data_type = ["klines", "aggr_trades"]

    dict_frames = {}
    min_date_numeric = datetime.combine(date(2020,1,1), datetime.min.time()).timestamp() * 1000
    ts_start_date_numeric = min_date_numeric
    # maximum timestamp available in Database is 00:00:00 of current day!
    max_date_numeric = datetime.combine(date.today(), datetime.min.time()).timestamp() * 1000


    for data_type in l_data_type:
        print("Fetching data on aggregate trades for today, please wait")
        df_res = binance_recent_data.handle_binance_recent_data(filename_output=None, api_key=api_key,
                                                                api_secret=api_sec, symbol=symbol_txt,
                                                                data_type=data_type,
                                                                timespan_av_min=int(min_date_numeric),
                                                                timespan_av_max=int(max_date_numeric),
                                                                ts_start_date_numeric=int(ts_start_date_numeric),
                                                                seconds_shift=30)

        if data_type == "aggr_trades":
            df_res = bf.fix_trades_dataset(df_res, 1)
        elif data_type == "klines":
            df_res = bf.fix_klines_dataset(df_res, 1)
        else:
            break
        dict_frames[data_type] = df_res

    return dict_frames


def create_derived_kpis():
    db_url = Settings.get_setting("db_conn")
    symbol = Settings.get_setting("coin_to_trade") + Settings.get_setting("fiat_curr")
    # look up symbol_id
    symbol_id =1
    approximate_avg_price = Settings.get_setting("approximate_avg_price")
    db_driver.create_derived_kpis(db_url, symbol_id, approximate_avg_price)

    # Exponentially weighted Moving Average
    ti.ewma(db_url, "ETHEUR", 50) #symbol_id = 1
    ti.ewma(db_url, "ETHEUR", 200)  # symbol_id = 1
    ti.ewma(db_url, "ETHEUR", 250)  # symbol_id = 1

    #Simple Moving average
    ti.simple_ma(db_url, "ETHEUR", 50)  # symbol_id = 1
    ti.simple_ma(db_url, "ETHEUR", 200)  # symbol_id = 1
    ti.simple_ma(db_url, "ETHEUR", 250)  # symbol_id = 1

    #RSI
    ti.rsi(db_url, "ETHEUR", 20)
    ti.rsi(db_url, "ETHEUR", 50)
    ti.rsi(db_url, "ETHEUR", 100)

    #Force Index
    ti.force_index(db_url, "ETHEUR", 20)
    ti.force_index(db_url, "ETHEUR", 50)
    ti.force_index(db_url, "ETHEUR", 100)

def get_min_and_max_dates(db_url, table_name):
    engine = create_engine(db_url)
    min_date_numeric = None
    max_date_numeric = None

    with engine.connect() as connection:
        try:
            if table_name == "f_aggr_trades":
                result = connection.execute(f'SELECT MIN(tx_time_numeric) , MAX(tx_time_numeric) FROM {table_name}')
            #TODO add numeric conversion of start time
            elif table_name == "f_klines":
                result = connection.execute(f'SELECT MIN(start_time) , MAX(start_time) FROM {table_name}')
            else:
                raise Exception

            for elem in result:
                min_date_numeric = elem[0]
                max_date_numeric = elem[1]
        except Exception as e:
            print(f"Error retrieving data: {e}")

    return min_date_numeric, max_date_numeric

#create_db()
#load_historical_data()


