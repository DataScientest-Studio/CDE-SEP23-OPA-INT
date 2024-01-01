from sqlalchemy import create_engine, Table, Column, Integer, Numeric, String, Boolean, MetaData, TIMESTAMP, inspect, insert
import pandas as pd

def create_kline_table_if_not_exists(db_url):
    engine = create_engine(db_url)
    metadata = MetaData()

    kline_table = Table('kline_data', metadata,
                        Column('start_time', TIMESTAMP(timezone=True)),
                        Column('open_price', Numeric),
                        Column('high_price', Numeric),
                        Column('low_price', Numeric),
                        Column('close_price', Numeric),
                        Column('volume', Numeric),
                        Column('close_time', TIMESTAMP(timezone=True)),
                        Column('quote_asset_volume', Numeric),
                        Column('number_of_trades', Integer),
                        Column('taker_buy_base_asset_volume', Numeric),
                        Column('taker_buy_quote_asset_volume', Numeric)
                        )

    inspector = inspect(engine)
    if not inspector.has_table('kline_data'):
        metadata.create_all(engine)
        print("Table 'kline_data' was created successfully.")
    else:
        print("Table 'kline_data' already exists.")


def insert_data_from_dataframe(df, db_url, table_name='public.kline_data'):
    df = df.drop(df.columns[-1], axis=1)
    df.columns = [
        'start_time', 'open_price', 'high_price', 'low_price',
        'close_price', 'volume', 'close_time', 'quote_asset_volume',
        'number_of_trades', 'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume'
    ]

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
        'quote_asset_volume': 'float',
        'number_of_trades': 'int',
        'taker_buy_base_asset_volume': 'float',
        'taker_buy_quote_asset_volume': 'float'
    })

    data = df.to_dict(orient='records')

    print(data)

    engine = create_engine(db_url, echo=True)
    with engine.connect() as connection:
        trans = connection.begin()
        try:
            stmt = insert(Table(table_name, MetaData(), autoload_with=engine)).values(data)
            connection.execute(stmt)
            print(f"Inserted {len(data)} records into '{table_name}'.")
            trans.commit()
        except Exception as e:
            print(f"Error {e}")
            trans.rollback()
