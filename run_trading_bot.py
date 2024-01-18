import json
import os
import time

from binance import ThreadedWebsocketManager
from binance.client import Client

import binance_recent_data
import binance_streams
import db_driver

# used: https://algotrading101.com/learn/binance-python-api-guide/
# https://readthedocs.org/projects/python-binance/downloads/pdf/latest/
# https://binance-docs.github.io/apidocs/spot/en/#exchange-information

settings_file_name = "settings.json"

def btc_trade_history(msg):
    ''' define how to process incoming WebSocket messages '''
    if msg['e'] != 'error':
        print(msg['c'])
        btc_price['last'] = msg['c']
        btc_price['bid'] = msg['b']
        btc_price['last'] = msg['a']
        btc_price['error'] = False
    else:
        btc_price['error'] = True


def load_settings(settings_file_name):
    with open(settings_file_name, "r") as settings_file:
        return json.load(settings_file)


# Read Settings
settings = load_settings(settings_file_name)
coin = settings["coin_to_trade"]
fiat_curr = settings["fiat_curr"]

# get Credentials
if settings["use_demo_account"]:
    flag_use_demo_acc = True
    api_key = settings["api_key_demo"]
    api_sec = settings["api_key_secret_demo"]
# Check if API credentials are stored in Environmental Variables
elif os.environ.get("binance_api") is not None and os.environ.get("binance_secret") is not None:
    flag_use_demo_acc = False
    api_key = os.environ.get("binance_api")
    api_sec = os.environ.get("binance_secret")
else:
    flag_use_demo_acc = False
    f = open("binance_api_key.txt", "r")
    creden = f.read()
    api_key = creden.split('\n')[0]
    api_sec = creden.split('\n')[1]
    f.close()

#Startup DB config
db_url = settings['db_conn']
db_driver.create_kline_table_if_not_exists(db_url)

# TODO: "build function that inserts historical data to database and checks whether already available"
# TODO: "get data from database"

# Get Recent Data
l_data_type = ["klines", "aggr_trades"]
symbol_txt = coin + fiat_curr
dict_frames = {}

# Data is Stored in txt file on disk and returned to df (in-memory)
for data_type in l_data_type:
    filename_output = settings[data_type]["filename_output"]
    range_start_col = settings[data_type]["range_start_cols"]
    range_end_col = settings[data_type]["range_end_cols"]
    df_res = binance_recent_data.handle_binance_recent_data(filename_output=filename_output, api_key=api_key,
                                                 api_secret=api_sec, symbol=symbol_txt,
                                                 range_start_cols=range_start_col, range_end_cols=range_end_col,
                                                 db_conn=db_url, data_type=data_type)
    dict_frames[data_type] = df_res

# Retrieve Data Stream
if settings["websocket_type"] != "async":
    bin_client = Client(api_key, api_sec, testnet=flag_use_demo_acc)
    # print(bin_client.get_account())
    print("Welcome")
    print("Your Account balance: ", bin_client.get_asset_balance(asset='BTC'))
    # Latest btc price
    btc_price = bin_client.get_symbol_ticker(symbol="BTCUSDT")
    print("The current BTC price is: ", btc_price)

    # init and start the WebSocket
    bsm = ThreadedWebsocketManager()
    bsm.start()
    bsm.start_symbol_ticker_socket(callback=btc_trade_history, symbol='BTCUSDT')
    # Going to sleep for 20s
    time.sleep(20)
    bsm.stop()
else:
    # from streamz import Stream
    # stream = Stream()
    # stream.map(query).accumulate(aggregate, start=0)
    binance_streams.run_main(api_key, api_sec, coin, fiat_curr, flag_use_demo_acc)
    # TODO: concatenate data frames

# line only inserted as debug point, has no value
settings = load_settings(settings_file_name)
