import db_driver
from settings import Settings
import pandas as pd
import binance_response_formatter as bf

#Startup DB config

def create_db():
    db_url = Settings.get_setting("db_conn")
    db_driver.create_db(db_url)

def load_historical_data():

    #TODO extend this method to read from multiple files
    df_aggregates = pd.read_csv('historical_data/aggtrades/aggtrades.csv', header=None)
    df_aggregates = bf.fix_trades_dataset(df_aggregates, 'ETHEUR')

    db_driver.insert_df_to_table(df_aggregates, Settings.get_setting("db_conn"), Settings.get_setting('aggregate_trades_table'))

    df_klines = pd.read_csv('historical_data/klines/klines.csv', header=None)
    df_klines = bf.fix_kleine_dataset(df_klines, 'ETHEUR')
    db_driver.insert_df_to_table(df_klines, Settings.get_setting("db_conn"), Settings.get_setting('klines_table'))

    df_symbol = pd.DataFrame({'symbol': 'ETHEUR'}, index=[0])
    db_driver.insert_df_to_table(df_symbol, Settings.get_setting("db_conn"), Settings.get_setting('symbols_table'))

create_db()
load_historical_data()


