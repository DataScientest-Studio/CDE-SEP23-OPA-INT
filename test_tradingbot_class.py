import json
import os


import load_data
import tradingbot as bot


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



#get symbol_id
symbol_id = load_data.load_symbol_id(symbol_txt)
# Make sure recent data (streaming table is loaded)
#dict_df_res = load_data.load_recent_data(api_key, api_sec, symbol_id)

tb = bot.TradingBot(api_key, api_sec, coin, fiat_curr, 1000, 10, 240, True)
tb.set_online(True)
symbol_txt = coin + fiat_curr
pred = tb.single_prediction()
test=tb.run_main()
test=1