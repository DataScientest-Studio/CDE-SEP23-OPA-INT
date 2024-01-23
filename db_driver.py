from sqlalchemy import create_engine, Table, Column, Integer, Numeric, String, Boolean, MetaData, TIMESTAMP, inspect, \
    insert, select


def create_db(db_url):
    engine = create_engine(db_url)
    metadata = MetaData()

    kline_table = Table('f_klines', metadata,
                        Column('start_time', TIMESTAMP(timezone=True)),
                        Column('open_price', Numeric),
                        Column('high_price', Numeric),
                        Column('low_price', Numeric),
                        Column('close_price', Numeric),
                        Column('volume', Numeric),
                        Column('close_time', TIMESTAMP(timezone=True)),
                        Column('number_of_trades', Integer),
                        Column('symbol', String)
                        )

    trades_table = Table('f_aggr_trades', metadata,
                            Column('agg_trade_id', Integer),
                         Column('price', Numeric),
                         Column('quantity', Numeric),
                         Column('transact_time', TIMESTAMP(timezone=True)),
                         Column('is_buyer_maker', Numeric),
                         Column('symbol', String)
                         )

    symbols_table = Table('d_symbols', metadata,
                          Column('symbol', String))

    inspector = inspect(engine)
    if not inspector.has_table('kline_data'):
        metadata.create_all(engine)

    else:
        print("Tables are created.")


def insert_df_to_table(df, db_url, table_name):
    data = df.to_dict(orient='records')
    insertTableData(db_url, table_name, data)

def insertTableData(db_url, table_name, data):
    engine = create_engine(db_url, echo=True)
    print(data)
    with engine.connect() as connection:
        trans = connection.begin()
        try:
            stmt = insert(Table(table_name, MetaData(), autoload_with=engine)).values(data)
            connection.execute(stmt)
            print(f"Inserted {len(data)} records into '{table_name}'.")
            trans.commit()
        except Exception as e:
            print(f"Rollback Error {e}")
            trans.rollback()


def getFirstTenRows(db_url, table_name):
    engine = create_engine(db_url, echo=True)

    with engine.connect() as connection:
        try:
            table = Table(table_name, MetaData(), autoload_with=engine)

            stmt = select(table).limit(10)

            result = connection.execute(stmt)

            rows = result.fetchall()

            for row in rows:
                print(row)

        except Exception as e:
            print(f"Error retrieving data: {e}")