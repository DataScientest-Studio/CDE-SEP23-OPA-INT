import json
from enum import Enum
from typing import Optional, Annotated

from binance.client import Client
from binance.exceptions import BinanceAPIException
from fastapi import FastAPI, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field

import tradingbot as bot
import api_settings

responses = {
    200: {"description": "Success"},
    400: {"description": "Wrong symbol format, check the symbol and try again."},
    401: {"description": "Time between Binance API and local server is off by more than 1000ms."}
}

# create API
api = FastAPI(
    title="Binance Trading Bot API",
    description="Use this API to access a trading bot that trades on the Binance Exchange. The trading bot is"
                " currently trained on the ETH/EUR pair and uses a LSTM model to predict future prices. The bot is "
                "intended for high frequency trading, e.g. holding Crypto for a couple of minutes. "
                "Forecasts are to be made for the next couple of minutes only. Increasing the forecast timespan "
                "will decrease the accuracy of the forecast."
                ,
    version="1.0.1")

security = HTTPBasic(description="Pass your credentials (api_key as username, api_sec as password)."
                                 "If you use local:file API will use credentials stored internally."
                                 "This is just a hack for the demo, do not use in production!")


# Credentials


api_key = api_settings.api_key_demo
api_sec = api_settings.api_key_secret_demo


class FiatCurrenciesAllowed(str, Enum):
    EUR = "EUR"
    USDT = "USDT"

class CryptoCurrenciesAllowed(str, Enum):
    ETH = "ETH"
    BTC = "BTC"

class single_forecast(BaseModel):
    coin: CryptoCurrenciesAllowed
    fiat_curr: FiatCurrenciesAllowed
    forecast_timespan: int = Field(5, title="Forecast Timespan in Minutes", ge=5, le=20)



class trading_bot_settings(BaseModel):
    coin: CryptoCurrenciesAllowed
    fiat_curr: FiatCurrenciesAllowed
    inv_amount: float = Field(1000, title="Investment Amount in Fiat Currency")
    forecast_timespan: int = Field(5, title="Forecast Timespan in Minutes", ge=5, le=20)
    timeout_sec: int = Field(180, title="Timeout in Seconds", ge=180, le=3600)

@api.get('/', name="Main, used for function test. Returns latest price", tags=["General"], responses=responses)
def get_price(symbol: str):
    bin_client = Client(api_key, api_sec, testnet=True)
    try:
        price = bin_client.get_symbol_ticker(symbol=symbol)
        return f"Binance API online, let me get the most recent price for you:", price, "."
    except BinanceAPIException as e:
        bin_client.close_connection()
        return JSONResponse(
            status_code = 400,
            content= {"error": f"Error: {e}, symbols are case sensitive and must be in the format 'ETHUSDT'."}
        )


@api.get('/wallet', name="Get funds in wallet", tags=["Wallet/ Portfolio"], responses=responses)
def get_wallet_balance(credentials: Annotated[HTTPBasicCredentials, Depends(security)],
                       coin: Optional[str]=None):
    if credentials and not (credentials.username == "local" and credentials.password == "file"):
        api_key = credentials.username
        api_sec = credentials.password
    else:
        f = open("./binance_api_key.txt", "r")
        creden = f.read()
        api_key = creden.split('\n')[0]
        api_sec = creden.split('\n')[1]

    bin_client = Client(api_key, api_sec, testnet=False)
    if coin:
        try:
            account_balances = bin_client.get_asset_balance(asset=coin)
        except BinanceAPIException as e:
            return JSONResponse(
                status_code=401,
                content={"error": f"Error: {e}, check local server time, it might be off by more than 1000ms."
                                  f" To fix it, synchronise datetime in your system settings."}
            )
        bin_client.close_connection()
        return f"For", {coin}, "your current balance is", account_balances, "."
    else:
        account_balances = bin_client.get_account()["balances"]
        bin_client.close_connection()
        return account_balances


@api.get('/bot_status', name="Check whether Bot is Online and Monitoring Markets", tags=["General"])
async def check_bot_status():
    try:
        # Read the bot status from a file
        with open('is_online.json', 'r') as file:
            bot_status = json.load(file)
            if bot_status is not None and bot_status is not False and bot_status !="false":
                return "Bot is online and monitoring markets"
            else:
                return "Bot is offline, you can start a new session!"
    except FileNotFoundError:
        return "Bot status file not found. Please start a bot session first."

@api.put('/forecast', name="Get forecast and investment decision", tags=["Investing"])
def get_forecast(forecast: single_forecast,
                 credentials: Annotated[HTTPBasicCredentials, Depends(security)]):

    timeout_sec = 60 # not explicitly needed here since it is a one time prediction
    inv_amount = 1000 # not explicitly needed here since it is a one time prediction
    symbol = forecast.coin + forecast.fiat_curr
    if credentials and not (credentials.username == "local" and credentials.password == "file"):
        api_key = credentials.username
        api_sec = credentials.password
    else:
        f = open("./binance_api_key.txt", "r")
        creden = f.read()
        api_key = creden.split('\n')[0]
        api_sec = creden.split('\n')[1]

    tb = bot.TradingBot(api_key, api_sec, forecast.coin, forecast.fiat_curr, inv_amount,
                        forecast.forecast_timespan, timeout_sec, True)
    tb.is_online = True

    try:
        bin_client = Client(api_key, api_sec, testnet=True)
        current_price = bin_client.get_symbol_ticker(symbol=symbol)["price"]
        bin_client.close_connection()
        prediction = tb.single_prediction()
        return f"Current Price: {current_price}, Predicted Price: {prediction}"
    except IndexError:
        return (f"Error: Could not make a forecast, please check whether a model exists for your symbol or"
                f" try again later.")


@api.put('/bot_session_start', name="Starts a bot session if bot is currently not running", tags=["Investing"])
def start_trading_bot(tbs: trading_bot_settings,
                      credentials: Annotated[HTTPBasicCredentials, Depends(security)]):

    if credentials and not (credentials.username == "local" and credentials.password == "file"):
        api_key = credentials.username
        api_sec = credentials.password
    else:
        f = open("./binance_api_key.txt", "r")
        creden = f.read()
        api_key = creden.split('\n')[0]
        api_sec = creden.split('\n')[1]

    try:
        # Read the bot status from a file
        with open('is_online.json', 'r') as file:
            bot_status = json.load(file)
            if bot_status is not None and (bot_status is True or bot_status =="true"):
                return f"Your bot is already online and monitoring markets, close it first!"
    except FileNotFoundError:
        pass
    try:
        tb = bot.TradingBot(api_key, api_sec, tbs.coin, tbs.fiat_curr, tbs.inv_amount, tbs.forecast_timespan,
                                tbs.timeout_sec, False)
        tb.set_online(True)
        return StreamingResponse(tb.main_stream(), media_type='text/event-stream')
    except:
        return f"An error occurred while starting the bot, please try again later."


@api.put('/bot_session_end', name="Manually interrupts a trading bot in case it is running", tags=["Investing"])
def stop_trading_bot(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    if credentials:
        api_key = credentials.username
        api_sec = credentials.password
    else:
        f = open("./binance_api_key.txt", "r")
        creden = f.read()
        api_key = creden.split('\n')[0]
        api_sec = creden.split('\n')[1]

    tb = bot.TradingBot(api_key, api_sec, "ETH", "EUR", 100,
                        5, 60, False)

    try:
        # Read the bot status from a file
        with open('is_online.json', 'r') as file:
            bot_status = json.load(file)
            if bot_status is not None and (bot_status is True or bot_status =="true"):
                tb = bot.TradingBot(api_key, api_sec, "ETH", "EUR", 100,
                                    5, 60, False)
                tb.set_online(False)
                return f"Closing open session..."
            else:
                return "Bot was already offline, you can start a new session!"
    except FileNotFoundError:
        return "Bot was already offline, you can start a new session!"


@api.get('/test')
async def test_stream(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    if credentials and not (credentials.username == "local" and credentials.password == "file"):
        api_key = credentials.username
        api_sec = credentials.password
    else:
        f = open("./binance_api_key.txt", "r")
        creden = f.read()
        api_key = creden.split('\n')[0]
        api_sec = creden.split('\n')[1]
    tb = bot.TradingBot(api_key, api_sec, "ETH", "EUR", 100, 5,180, False)
    tb.set_online(True)
    return StreamingResponse(tb.main_stream(), media_type='text/event-stream')
