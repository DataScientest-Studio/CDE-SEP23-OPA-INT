""" Use this code to run the trading bot in a static environment, meaning no need to
pass continuous stream back through API """

import asyncio
import os, time
import pandas as pd
import binance_response_formatter as bf
import db_driver
import ml_training as ml_train
import json

from settings import Settings
from binance import AsyncClient, BinanceSocketManager, Client
from load_data import load_symbol_id, load_recent_data, create_derived_kpis, load_data_from_db_table

import contextlib
import io

os.environ['TZ'] = 'UTC' # set timezone to UTC
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

# Define which keys to keep from streams
keys_candles = ["t","o", "h", "l", "c", "v", "T", "q", "n", "B", "Q", "s"]
db_url = Settings.get_setting("db_conn")

async_sleep_sec = 30

class TradingBot:
    def __init__(self, api_key, api_secret, coin, fiat_curr, inv_amount, forecast_timespan, timeout_sec, flag_use_demo_acc):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = None
        self.coin = coin
        self.fiat_curr = fiat_curr
        self.inv_amount = inv_amount
        self.position_fiat = inv_amount
        self.position_crypto = 0
        self.flag_use_demo_acc = flag_use_demo_acc
        self.forecast_timespan = forecast_timespan
        self.timeout_sec = timeout_sec
        self.symbol_txt = coin + fiat_curr
        self.is_online = self.load_is_online()
        self.is_invested_in_crypto = False

    def check_online(self):
        try:
            self.run()
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False

    def load_is_online(self):
        try:
            with open('is_online.json', 'r') as f:
                self.is_online = json.load(f)
        except FileNotFoundError:
            self.is_online = False
        return self.is_online

    def save_is_online(self, online):
        with open('is_online.json', 'w') as f:
            json.dump(online, f)

    def set_online(self, online):
        self.is_online = online
        self.save_is_online(online)

    def set_offline(self):
        self.is_online = False
        with open('is_online.json', 'w') as f:
            json.dump(False, f)


    def close_connection(self):
        try:
            self.client.close_connection()
            return "Connection closed successfully!"
        except:
            return "An error occurred while closing the connection!"

    async def make_trade(self, api_key, api_secret, symbol, symbol_id, start_time, inv_amount,
                         forecast_timespan, timeout_sec, ml_model, ml_scaler):
        while (time.time() - start_time < timeout_sec) and self.load_is_online() == True:
            trade = await self.make_investment_decision(api_key, api_secret, symbol, symbol_id,
                                           forecast_timespan, ml_model, ml_scaler)

            if time.time() - start_time > max(60, timeout_sec):
                print(f"Closing Bot", max(60, timeout_sec), "seconds")
                print("Your Fiat currency balance: ", self.position_fiat, "Your Crypto balance: ", self.position_crypto)
                self.set_online(False)
                break

            if self.is_invested_in_crypto == False and trade == "BUY":
                print(f"Buying Decision is positive, new balance in", self.coin, "is", self.position_crypto)
                self.is_invested_in_crypto = True
                print(f"Going to sleep for {async_sleep_sec} seconds")
                await asyncio.sleep(async_sleep_sec)
                pass
            elif self.is_invested_in_crypto == False and trade == "HOLD":
                print("HOLD - No Action")
                print(f"Going to sleep for {async_sleep_sec} seconds")
                await asyncio.sleep(async_sleep_sec)
                pass
            elif self.is_invested_in_crypto == True and trade == "SELL":
                print(f"Selling Decision is positive, new balance in", self.fiat_curr, "is", self.position_fiat)
                self.is_invested_in_crypto = False
                print(f"Going to sleep for {async_sleep_sec} seconds")
                await asyncio.sleep(async_sleep_sec)
                pass
            elif self.is_invested_in_crypto == True and trade == "HOLD":
                print("HOLD - No Action")
                print(f"Going to sleep for {async_sleep_sec} seconds")
                await asyncio.sleep(async_sleep_sec)
                pass
            else:
                print(f"Not investing in crypto at the moment!. Rechecking in {async_sleep_sec} seconds")
                await asyncio.sleep(async_sleep_sec)
                pass


    async def make_investment_decision(self, api_key, api_secret, symbol, symbol_id,
                                       forecast_timespan, ml_model, ml_scaler):
        # Apply ML model to check whether we should invest!
        bin_client = Client(api_key, api_secret, testnet=True)
        current_price = bin_client.get_symbol_ticker(symbol=symbol)["price"]
        bin_client.close_connection()
        print("Create KPIs and predict price")
        df_klines = load_data_from_db_table(db_url, Settings.get_setting("klines_stream_table"))
        df_input_prediction = create_derived_kpis(df_klines, symbol_id)
        predicted_price = ml_train.get_predicted_data(ml_model, ml_scaler, df_input_prediction, symbol_id, forecast_timespan)
        print("Current Price: ", current_price, "Predicted Price: ", float(predicted_price))

        # A buy decision is positive if the predicted price is higher than the current price!
        if self.is_invested_in_crypto == False:
            if float(predicted_price) > float(current_price):
                self.position_crypto = self.position_fiat / float(current_price)
                self.position_fiat = 0
                return "BUY"
            else:
                return "HOLD"
        else:

            if float(predicted_price) < float(current_price):
                self.position_fiat = self.position_crypto * float(current_price)
                self.position_crypto= 0
                return "SELL"
            else:
                return "HOLD"




    async def get_kline_data(self, bsm, symbol, start_time, timeout_sec):

        symbol_multi_socket = symbol.lower() + '@kline_1m'
        async with bsm.multiplex_socket([symbol_multi_socket]) as stream:
            while self.load_is_online():
                res = await stream.recv()
                res_dict = res["data"]["k"]

                res_dict_selected_keys = {key: res_dict[key] for key in keys_candles}
                res_df = pd.DataFrame(res_dict_selected_keys, index=pd.Series(res_dict["s"]))
                res_df = bf.fix_klines_dataset(res_df, 1)
                db_driver.insert_df_to_table(res_df, db_url, Settings.get_setting("klines_stream_table"))
                if time.time() - start_time > timeout_sec:
                    print(f"Closing klines stream after {timeout_sec} seconds")
                    break
                # klines stream only every 60 seconds
                await asyncio.sleep(60)
                # print(res_df)




    async def main(self, api_key, api_secret, coin, fiat_curr, inv_amount,
                   forecast_timespan, timeout_sec, flag_use_demo_acc):

        self.client = await AsyncClient.create(api_key, api_secret, testnet=flag_use_demo_acc)
        bsm = BinanceSocketManager(self.client, user_timeout=20)

        symbol_txt = coin + fiat_curr
        symbol_id = load_symbol_id(symbol_txt)

        model_file_name = ml_train.get_valid_model(db_url, symbol_id)[0]
        ml_model = ml_train.load_model_file(model_file_name)

        scaler_file_name = model_file_name.replace("model", "scaler").replace("keras", "bin")
        ml_scaler = ml_train.load_scaler_file(scaler_file_name)

        start_time = time.time()

        await asyncio.gather(
            self.make_trade(api_key, api_secret, symbol_txt,symbol_id, start_time,
                            inv_amount, forecast_timespan, timeout_sec,
                            ml_model, ml_scaler),
            self.get_kline_data(bsm, symbol_txt, start_time, timeout_sec))

        await asyncio.sleep(10)
        await self.client.close_connection()

    def run_main(self):
        return asyncio.run(self.main(self.api_key, self.api_secret, self.coin,
                                     self.fiat_curr, self.inv_amount,
                                     self.forecast_timespan, self.timeout_sec, self.flag_use_demo_acc))

    def single_prediction(self):
        symbol_txt = self.coin + self.fiat_curr
        symbol_id = load_symbol_id(symbol_txt)

        model_file_name = ml_train.get_valid_model(db_url, symbol_id)[0]
        scaler_file_name = model_file_name.replace("model", "scaler").replace("keras", "bin")

        model = ml_train.load_model_file(model_file_name)
        scaler = ml_train.load_scaler_file(scaler_file_name)

        dict_df_res = load_recent_data(self.api_key, self.api_secret, symbol_id)
        df_input_prediction = create_derived_kpis(dict_df_res["klines"], symbol_id, create_from_predictions=False)
        return ml_train.get_predicted_data(model, scaler, df_input_prediction, symbol_id, holding_period=10)