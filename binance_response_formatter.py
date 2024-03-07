import pandas as pd

def fix_klines_dataset(df, symbol_id):
    df = df.drop(df.columns[-1], axis=1)
    df.columns = [
        'start_time', 'open_price', 'high_price', 'low_price',
        'close_price', 'volume', 'close_time', 'quote_asset_volume',
        'number_of_trades', 'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume'
    ]

    df.drop(['taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'quote_asset_volume'], axis=1, inplace=True)
    # Add Columns
    df['symbol_id'] = symbol_id
    df["start_time_numeric"] = df["start_time"]
    df["close_time_numeric"] = df["close_time"]

    # Transform numeric to Dates
    df['start_time'] = pd.to_datetime(df['start_time'], unit='ms', utc=True)
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms', utc=True)

    df['start_time'] = df['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['close_time'] = df['close_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df = df.astype({
        'open_price': 'float',
        'high_price': 'float',
        'low_price': 'float',
        'close_price': 'float',
        'volume': 'float',
        'number_of_trades': 'int'
    })

    return df

def fix_trades_dataset(df, symbol_id):
    #df = df.drop(df.columns[-1], axis=1)
    df.columns = [
        'agg_trade_id', 'price', 'quantity', 'first_trade_id',
        'last_trade_id', 'transact_time', 'is_buyer_maker', "best_price_match"]
    
    df.drop(['first_trade_id', 'last_trade_id'], axis=1, inplace=True)
    df['tx_time_numeric'] = df['transact_time']
    df['transact_time'] = pd.to_datetime(df['transact_time'], unit='ms', utc=True)
    df['transact_time'] = df['transact_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df = df.astype({
        'agg_trade_id': 'int',
        'price': 'float',
        'quantity': 'float',
        "is_buyer_maker": 'int',
        "best_price_match": 'int'
    })

    df['symbol_id'] = symbol_id

    return df

df_symbol = pd.DataFrame({'symbol': 'ETHEUR'}, index=[0])
print(df_symbol.head(5))

