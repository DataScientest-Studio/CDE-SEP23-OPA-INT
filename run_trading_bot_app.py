import json
import os
import time

from binance import ThreadedWebsocketManager
from binance.client import Client

import plotly.graph_objects as go
import binance_streams
import load_data
import ml_train
import streamlit as st


def load_settings(settings_file_name):
    with open(settings_file_name, "r") as settings_file:
        return json.load(settings_file)


# Read Standard Settings
settings_file_name = "settings.json"
settings = load_settings(settings_file_name)
coin = settings["coin_to_trade"]
fiat_curr = settings["fiat_curr"]
symbol_txt = coin + fiat_curr
load_historical_data = settings["load_data"]["load_historical"]
recreate_tables = settings["load_data"]["recreate_tables"]
train_ml = settings["model_fitting"]["train_ml"]
db_url = settings["db_conn"]
klines_stream_table = settings["klines_stream_table"]

# get Credentials
def get_credentials():
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
    return api_key, api_sec

# Startup DB config

if recreate_tables == "True":
    load_data.create_db()

#get symbol_id
def get_symbol_id(symbol_txt):
    return load_data.load_symbol_id(symbol_txt)


# Get Recent Data
l_data_type = ["klines", "aggr_trades"]

# Create derived KPIs
#TODO move this into the load_recent_data or get historical data function
#load_data.create_derived_kpis()



#TODO: concatenate previous data frame with stream and make investment decision
# Retrieve Data Stream

def get_data_from_socket(api_key, api_sec, flag_use_demo_acc):
    if settings["websocket_type"] != "async":
        bin_client = Client(api_key, api_sec, testnet=flag_use_demo_acc)
        # print(bin_client.get_account())
        # print("Welcome")
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
        binance_streams.run_main(api_key, api_sec, coin, fiat_curr, flag_use_demo_acc)





def run_app():
    st.title('Binance Trading Bot - OPA Project')

    flag_use_demo_acc = True  # or False
    symbol_id = load_data.load_symbol_id(symbol_txt)
    api_key, api_sec = get_credentials()

    # Initialize session state for buttons if not already done
    if 'reload_data' not in st.session_state:
        st.session_state['reload_data'] = False
    if 'keep_data' not in st.session_state:
        st.session_state['keep_data'] = False

    # Load historical data if not yet available in database
    # Historical data can be loaded from disk (fast) or through API calls (slow)
    st.write("Do you want to (re)-load historical data?")
    col_yes, col_no = st.columns([1, 1])
    with col_yes:
        if st.button('Yes, reload'):
            st.session_state['reload_data'] = True
    with col_no:
        if st.button('No, keep data as is'):
            st.session_state['keep_data'] = True

    if st.session_state['reload_data']:
        st.write("Loading historical data...")
        load_data.load_historical_data(api_key, api_sec, symbol_id)
    elif st.session_state['keep_data']:
        st.write("Keeping historical data as is...")

    if st.session_state['reload_data'] or st.session_state['keep_data']:
        # Ask the user if they want to retrain the machine learning model
        st.write("Do you want to (re)-train the machine learning model?")
        col_yes_train, col_no_train = st.columns([1, 1])
        with col_yes_train:
            st.session_state['retrain_model'] = st.button('Yes, re-train model')
        with col_no_train:
            st.session_state['keep_model'] = st.button('No, use existing model')

        # If train_ml is True, Machine Learning Model is (re)trained and a new model is stored on disk and
        # metadata in database
        if st.session_state['retrain_model']:
            # Call the function to retrain the machine learning model
            st.write("Start Training of Machine Learning Model, this may take a while.")
            ml_train.get_data(settings["db_conn"], settings["kpi_table"], symbol_id)
            st.write("Machine Learning Model trained.")

        if st.session_state['retrain_model'] or st.session_state['keep_model']:
            # get Recent data (klines and aggr_trades), recent = since noon today
            st.write("Loading recent data, this may take a while.")
            dict_df_res = load_data.load_recent_data(api_key, api_sec, symbol_id)
            st.write("Recent data loaded.")

            # TODO: apply ML model to data from stream
            st.write("Loading valid Machine Learning Model.")
            model_file_name = ml_train.get_valid_model(db_url, symbol_id)[0]
            scaler_file_name = model_file_name.replace("model", "scaler").replace("keras", "bin")
            st.write("Machine Learning Model loaded from Disk, ready to make predictions.")

            st.write("Making predictions from recent data.")
            df_input_prediction = load_data.create_derived_kpis(dict_df_res["klines"], symbol_id)
            y_pred = ml_train.get_predicted_data(model_file_name, df_input_prediction, scaler_file_name)
            inv_decision = ml_train.make_investment_decision(y_pred)
            st.write("Investment Decision: ", inv_decision)

            get_data_from_socket(api_key, api_sec, flag_use_demo_acc)
            data = load_data.load_data_from_db_table(db_url, klines_stream_table)

            # Display the data
            st.dataframe(data)

            # Create a Candlestick Chart
            fig = go.Figure(data=[go.Candlestick(x=data["start_time"],
                                                             open=data['open_price'],
                                                             high=data['high_price'],
                                                             low=data['low_price'],
                                                             close=data['close_price'])])
            #pl = st.empty()
            st.plotly_chart(fig)




if __name__ == "__main__":
    run_app()