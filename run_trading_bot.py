import os
import time
import json
from binance.client import Client
from binance import ThreadedWebsocketManager
import binance_streams
#used: https://algotrading101.com/learn/binance-python-api-guide/
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

def query(df):
    return df[df.name == 'Alice']

def aggregate(acc, df):
    return acc + df.amount.sum()


# Read Settings
settings = load_settings(settings_file_name)

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


if settings["websocket_type"] != "async":
    bin_client = Client(api_key, api_sec, testnet=flag_use_demo_acc)
    #print(bin_client.get_account())
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
    binance_streams.run_main(api_key, api_sec, settings["coin_to_trade"], settings["fiat_curr"], flag_use_demo_acc)

settings = load_settings(settings_file_name)
