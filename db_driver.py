from sqlalchemy import create_engine, Table, Column, Integer, Numeric, String, MetaData, TIMESTAMP, \
    insert, select, PrimaryKeyConstraint, ForeignKey
import pandas as pd
import numpy as np

def create_db(db_url):
    engine = create_engine(db_url)
    metadata = MetaData()

    symbols_table = Table('d_symbols', metadata,
                          Column('symbol_id', Integer, primary_key=True, autoincrement=True),
                          Column('symbol', String))

    kline_table = Table('f_klines', metadata,
                        Column('start_time', TIMESTAMP(timezone=True)),
                        Column('open_price', Numeric),
                        Column('high_price', Numeric),
                        Column('low_price', Numeric),
                        Column('close_price', Numeric),
                        Column('volume', Numeric),
                        Column('close_time', TIMESTAMP(timezone=True)),
                        Column('number_of_trades', Integer),
                        Column('symbol_id', Integer, ForeignKey('d_symbols.symbol_id')),
                        Column('close_time_numeric', Numeric),
                        Column('start_time_numeric', Numeric),
                        PrimaryKeyConstraint('start_time', 'close_time', name='kline_pk')
                        )

    klines_stream_table = Table('f_klines_stream', metadata,
                                Column('start_time', TIMESTAMP(timezone=True)),
                                Column('open_price', Numeric),
                                Column('high_price', Numeric),
                                Column('low_price', Numeric),
                                Column('close_price', Numeric),
                                Column('volume', Numeric),
                                Column('close_time', TIMESTAMP(timezone=True)),
                                Column('number_of_trades', Integer),
                                Column('symbol_id', Integer, ForeignKey('d_symbols.symbol_id')),
                                Column('close_time_numeric', Numeric),
                                Column('start_time_numeric', Numeric),
                                PrimaryKeyConstraint('start_time', 'close_time', name='kline_stream_pk')
                                )

    trades_table = Table('f_aggr_trades', metadata,
                         Column('agg_trade_id', Integer),
                         Column('price', Numeric),
                         Column('quantity', Numeric),
                         Column('transact_time', TIMESTAMP(timezone=True)),
                         Column('is_buyer_maker', String),
                         Column('best_price_match', String),
                         Column('tx_time_numeric', Numeric),
                         Column('symbol_id', Integer, ForeignKey('d_symbols.symbol_id')),
                         PrimaryKeyConstraint('agg_trade_id', 'tx_time_numeric', 'symbol_id',
                                              name='pk_aggrtrades')
                         )

    trades_stream_table = Table('f_aggr_trades_stream', metadata,
                                Column('agg_trade_id', Integer),
                                Column('price', Numeric),
                                Column('quantity', Numeric),
                                Column('transact_time', TIMESTAMP(timezone=True)),
                                Column('is_buyer_maker', String),
                                Column('best_price_match', String),
                                Column('tx_time_numeric', Numeric),
                                Column('symbol_id', Integer, ForeignKey('d_symbols.symbol_id')),
                                PrimaryKeyConstraint('agg_trade_id', 'tx_time_numeric', 'symbol_id',
                                                     name='pk_aggrtrades_stream')
                                )

    dvkpis_table = Table('f_dvkpi', metadata,
                         Column('dvkpi_timestamp', TIMESTAMP(timezone=True)),
                         Column('dvkpi_symbol_id', Integer, ForeignKey('d_symbols.symbol_id')),
                         Column('dvkpi_kpi', String),
                         Column('dvkpi_kpi_value', Numeric),
                         PrimaryKeyConstraint('dvkpi_timestamp', 'dvkpi_symbol_id', 'dvkpi_kpi', name='pk_kpis')
                         )

    dvkpis_stream_table = Table('f_dvkpi_stream', metadata,
                                Column('dvkpi_timestamp', TIMESTAMP(timezone=True)),
                                Column('dvkpi_symbol_id', Integer, ForeignKey('d_symbols.symbol_id')),
                                Column('dvkpi_kpi', String),
                                Column('dvkpi_kpi_value', Numeric),
                                PrimaryKeyConstraint('dvkpi_timestamp', 'dvkpi_symbol_id', 'dvkpi_kpi',
                                                     name='pk_kpis_stream')
                                )

    models_table = Table('d_models', metadata,
                         Column('model_timestamp', TIMESTAMP(timezone=True)),
                         Column('model_id', Integer),
                         Column('model_active', String),
                         Column('model_filename', String),
                         Column('symbol_id', Integer, ForeignKey('d_symbols.symbol_id')),
                         PrimaryKeyConstraint('model_id', 'symbol_id', name='pk_models')
                         )

    try:
        metadata.create_all(engine, checkfirst=True)
    except Exception as e:
        print(f"Error creating tables: {e}")

def insert_df_to_table(df, db_url, table_name):
    data = df.to_dict(orient='records')
    insertTableData(db_url, table_name, data)


def insertTableData(db_url, table_name, data):
    engine = create_engine(db_url, echo=False)

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


def delete_data_from_table(db_url, table_name, symbol_id):
    engine = create_engine(db_url, echo=False)
    with engine.connect() as connection:
        trans = connection.begin()
        try:
            table = Table(table_name, MetaData(), autoload_with=engine)
            if table_name in ("f_dvkpi_stream", "f_dvkpi"):
                stmt = table.delete().where(table.c.dvkpi_symbol_id == int(symbol_id))
            else:
                stmt = table.delete().where(table.c.symbol_id == int(symbol_id))
            connection.execute(stmt)
            trans.commit()
            print(f"Deleted data from '{table_name}' for symbol_id {symbol_id}.")
        except Exception as e:
            print(f"Error deleting data: {e}")


# Manipulates
def create_derived_kpis(db_url, symbol_id, approximate_avg_price, df_klines):
    engine = create_engine(db_url, echo=False)

    # Calculate average prices first
    with engine.connect() as connection:
        try:
            if approximate_avg_price == "True":
                if df_klines is None:
                    klines = Table("f_klines", MetaData(), autoload_with=engine)
                    stmt = select(klines)
                    result = connection.execute(stmt)
                else:
                    result = df_klines
                result_df = pd.DataFrame(result)
                result_df = result_df.reset_index()
                result_df["dvkpi_kpi_value"] = result_df[["open_price", "close_price"]].mean(axis=1)
                result_df["dvkpi_kpi"] = "AVG_PRICE"
                result_df["dvkpi_symbol_id"] = symbol_id
                result_df = result_df[["start_time", "close_time", "dvkpi_kpi_value", "dvkpi_kpi", "dvkpi_symbol_id"]]
                result_df.columns = ["start_time", "dvkpi_timestamp", "dvkpi_kpi_value", "dvkpi_kpi", "dvkpi_symbol_id"]
                result_df = result_df[["dvkpi_timestamp", "dvkpi_kpi", "dvkpi_kpi_value", "dvkpi_symbol_id"]]
                # replace NaN with None
                result_df["dvkpi_kpi_value"] = result_df["dvkpi_kpi_value"].replace(np.nan, None)


            else:
                # TODO add join based on local dataframe (df_klines) if df_klines is not None
                klines = Table("f_klines", MetaData(), autoload_with=engine)
                aggrtrades = Table("f_aggr_trades", MetaData(), autoload_with=engine)
                query = (
                    select(klines.c.start_time, klines.c.close_time, aggrtrades.c.quantity, aggrtrades.c.price)
                    .select_from(klines.outerjoin(aggrtrades,
                                                  aggrtrades.c.transact_time.between(klines.c.start_time,
                                                                                     klines.c.close_time)))
                )

                result = connection.execute(query)
                result_df = pd.DataFrame(result)
                result_df = result_df.groupby(["start_time", "close_time"]).apply(
                    lambda x: np.average(x.price, weights=x.quantity))
                result_df = result_df.reset_index()
                result_df["dvkpi_kpi"] = "AVG_PRICE"
                result_df["dvkpi_symbol_id"] = symbol_id
                result_df.columns = ["start_time", "dvkpi_timestamp", "dvkpi_kpi_value", "dvkpi_kpi", "dvkpi_symbol_id"]
                result_df = result_df[["dvkpi_timestamp", "dvkpi_kpi", "dvkpi_kpi_value", "dvkpi_symbol_id"]]
                # replace NaN with None
                result_df["dvkpi_kpi_value"] = result_df["dvkpi_kpi_value"].replace(np.nan, None)

        except Exception as e:
            print(f"Error calculating average prices for derived KPIs: {e}")

    return result_df


def filter_derived_kpis(db_url, symbol_id, df):
    engine = create_engine(db_url, echo=False)
    with engine.connect() as connection:
        try:
            # determine latest data point in database table dvkpi
            tab_dvkpi = Table("f_dvkpi", MetaData(), autoload_with=engine)
            stmt = select(tab_dvkpi.c.dvkpi_timestamp).where((tab_dvkpi.c.dvkpi_kpi == 'AVG_PRICE'
                                                              and tab_dvkpi.c.dvkpi_symbol_id == symbol_id))
            result = connection.execute(stmt)
            max_timestamp = pd.DataFrame(result).max()[0]
            max_timestamp = pd.to_datetime(max_timestamp, format='%Y-%m-%d %H:%M:%S', utc=True)  # .to_datetime64()

            # Filter result_df for only new data
            df['dvkpi_timestamp'] = pd.to_datetime(df['dvkpi_timestamp'], format='%Y-%m-%d %H:%M:%S')
            df = df[df["dvkpi_timestamp"] > max_timestamp]
            return df

        except Exception as e:
            print(f"Error filtering derived KPIs: {e}")


def get_data_from_db_table(db_url, table_name, symbol_id_num):
    engine = create_engine(db_url, echo=False)
    with engine.connect() as connection:
        try:
            tab_to_load = Table(table_name, MetaData(), autoload_with=engine)
            if table_name == "kpi_stream_table":
                # TODO check why adding where condition yields Error retrieving data:
                # Boolean value of this clause is not defined
                stmt = select(tab_to_load)
            else:
                stmt = select(tab_to_load)
            result = connection.execute(stmt)
            rows = result.fetchall()
            return rows
        except Exception as e:
            print(f"Error retrieving data: {e}")


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


