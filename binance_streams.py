from binance import AsyncClient, BinanceSocketManager, Client
import asyncio
import pandas as pd

# Define which keys to keep from streams
keys_candles = ["t","o", "h", "l", "c", "V"]
key_agg_trades = ["a", "p", "q", "f", "l", "T"]
colnames_agg_trades = ["AggTradeID","Price","Quantity","FirstTradeID","LastTradeID","Timestamp"]
def ask_exit(signame, loop):
    print("got signal %s: exit" % signame)
    loop.stop()

async def get_kline_data(bsm,api_key, api_secret, symbol):
    try:
        file = open("kline_data.txt", "x")
    except:
        file = open("kline_data.txt", "w")
    bin_client = Client(api_key, api_secret)
    df_recent = pd.DataFrame(bin_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=1000))
    df_recent = df_recent.iloc[:,0:6]

    with open("kline_data.txt", "a") as file:
        file.write(df_recent.to_string(header=False, index=False))

    symbol_multi_socket = symbol.lower() + '@kline_1m'
    async with bsm.multiplex_socket([symbol_multi_socket]) as stream:
        while True:
            res = await stream.recv()
            res_dict = res["data"]["k"]
            res_dict_selected_keys = {key: res_dict[key] for key in keys_candles}
            res_df = pd.DataFrame(res_dict_selected_keys, index=pd.Series(res_dict["s"]))
            with open('kline_data.txt', 'a') as file:
                file.write(f'\n')
                file.write(res_df.to_string(header=False,index=False))
                file.close()
            print(res)



async def get_aggr_trade_data(bsm,api_key, api_secret, symbol):
    try:
        file = open("aggr_trades.txt", "x")
    except:
        file = open("aggr_trades.txt", "w")
    bin_client = Client(api_key, api_secret)
    df_recent = pd.DataFrame(bin_client.get_aggregate_trades(symbol=symbol, limit=1000))
    df_recent = df_recent.iloc[:,0:6]

    with open("aggr_trades.txt", "a") as file:
        file.write(df_recent.to_string(header=False, index=False))

    symbol_multi_socket = symbol.lower() + '@aggTrade'
    async with bsm.multiplex_socket([symbol_multi_socket]) as stream:
        while True:
            res = await stream.recv()
            res_dict = res["data"]
            res_dict_selected_keys = {key: res_dict[key] for key in key_agg_trades}
            res_df = pd.DataFrame(res_dict_selected_keys, index=pd.Series(res_dict["s"]))
            with open('aggr_trades.txt', 'a') as file:
                file.write(f'\n')
                file.write(res_df.to_string(header=False,index=False))
                file.close()
            print(res)

async def main(api_key, api_secret, coin,fiat_curr, flag_use_demo_acc):
    client = await AsyncClient.create(api_key, api_secret, testnet=flag_use_demo_acc)
    bsm = BinanceSocketManager(client, user_timeout=20)

    symbol_txt = coin + fiat_curr
    await asyncio.gather(get_kline_data(bsm,api_key, api_secret, symbol_txt),
                         get_aggr_trade_data(bsm,api_key, api_secret, symbol_txt))
    await asyncio.sleep(10)
    await client.close_connection()

def run_main(api_key, api_secret, coin="ETH", fiat_curr="USDT",flag_use_demo_acc=True):
    return asyncio.run(main(api_key, api_secret, coin, fiat_curr, flag_use_demo_acc))

#if __name__ == "__main__":
#asyncio.run(main())
