import json
import os
import time

from binance import ThreadedWebsocketManager
from binance.client import Client

import binance_streams
import load_data
import ml_train


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
settings_file_name = "settings.json"
settings = load_settings(settings_file_name)
coin = settings["coin_to_trade"]
fiat_curr = settings["fiat_curr"]
symbol_txt = coin + fiat_curr
load_historical_data = settings["load_data"]["load_historical"]
recreate_tables = settings["load_data"]["recreate_tables"]
train_ml = settings["model_fitting"]["train_ml"]
db_url = settings["db_conn"]

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

# Startup DB config

if recreate_tables == "True":
    load_data.create_db()

#get symbol_id
symbol_id = load_data.load_symbol_id(symbol_txt)


# Load historical data if not yet available in database
# Historical data can be loaded from disk (fast) or through API calls (slow)
if load_historical_data == "True":
    load_data.load_historical_data(api_key, api_sec, symbol_id)
    load_data.create_derived_kpis()


# If train_ml is True, Machine Learning Model is (re)trained and a new model is stored on disk and metadata in database
if train_ml == "True":
    ml_train.get_data(settings["db_conn"], settings["kpi_table"], symbol_id)



# Get Recent Data
l_data_type = ["klines", "aggr_trades"]

# get Recent data (klines and aggr_trades), recent = since noon today
dict_df_res = load_data.load_recent_data(api_key, api_sec, symbol_id)

#TODO write backup function -> in case that model is not stored in database but available on disk, we should pick the lastest one from disk
model_file_name = ml_train.get_valid_model(db_url, symbol_id)[0]
scaler_file_name = model_file_name.replace("model", "scaler").replace("keras", "bin")
df_input_prediction = load_data.create_derived_kpis(dict_df_res["klines"], symbol_id)
y_pred = ml_train.get_predicted_data(model_file_name, df_input_prediction, scaler_file_name)
inv_decision = ml_train.make_investment_decision(y_pred)

#TODO: concatenate previous data frame with stream and make investment decision
# Retrieve Data Stream
if settings["websocket_type"] != "async":
    bin_client = Client(api_key, api_sec, testnet=flag_use_demo_acc)
    # print(bin_client.get_account())
    #print("Welcome")
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
    # TODO: apply ML model to data from stream
    binance_streams.run_main(api_key, api_sec, coin, fiat_curr, flag_use_demo_acc)