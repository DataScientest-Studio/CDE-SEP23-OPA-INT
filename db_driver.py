from sqlalchemy import create_engine, Table, Column, Integer, Numeric, String, Boolean, MetaData, TIMESTAMP, inspect, \
    insert, select, text, PrimaryKeyConstraint, ForeignKey
import pandas as pd
import numpy as np

def create_db(db_url, create_dimensions_first=False):
    engine = create_engine(db_url)
    metadata = MetaData()

    if create_dimensions_first:
        symbols_table = Table('d_symbols', metadata,
                              Column('symbol_id', Integer, primary_key=True, autoincrement=True),
                              Column('symbol', String))
    else:
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

        trades_table = Table('f_aggr_trades', metadata,
                                Column('agg_trade_id', Integer),
                             Column('price', Numeric),
                             Column('quantity', Numeric),
                             Column('transact_time', TIMESTAMP(timezone=True)),
                             Column('is_buyer_maker', String),
                             Column('best_price_match', String),
                             Column('tx_time_numeric', Numeric),
                             Column('symbol_id', Integer, ForeignKey('d_symbols.symbol_id')),
                             PrimaryKeyConstraint('agg_trade_id', 'tx_time_numeric', 'symbol_id', name='pk_aggrtrades')
                             )

        dvkpis_table = Table('f_dvkpi', metadata,
                                Column('dvkpi_timestamp', TIMESTAMP(timezone=True)),
                             Column('dvkpi_symbol_id', Integer, ForeignKey('d_symbols.symbol_id')),
                             Column('dvkpi_kpi', String),
                             Column('dvkpi_kpi_value', Numeric),
                             PrimaryKeyConstraint('dvkpi_timestamp', 'dvkpi_symbol_id', 'dvkpi_kpi', name='pk_kpis')
                             )


    inspector = inspect(engine)
    #TODO extend check to other tables
    #TODO put this check up in front
    if not create_dimensions_first:
        if not (inspector.has_table('f_klines')
                and inspector.has_table('f_aggr_trades')
                and inspector.has_table('f_dvkpi')):
            metadata.create_all(engine, checkfirst=True)
            print("FactTables are created.")
        else:
            print("Fact tables had already been created.")
    else:
        if not inspector.has_table('d_symbols'):
                metadata.create_all(engine, checkfirst=True)
                print("Dimension Tables are created.")
        else:
            print("Dimension tables had already been created.")


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


def create_derived_kpis(db_url, symbol_id, approximate_avg_price):
    engine = create_engine(db_url, echo=True)

    # Calculate average prices first
    with engine.connect() as connection:
        try:
            if approximate_avg_price == "True":
                klines = Table("f_klines", MetaData(), autoload_with=engine)
                stmt = select(klines)
                result = connection.execute(stmt)
                result_df = pd.DataFrame(result)
                result_df = result_df.reset_index()
                result_df["dvkpi_kpi_value"] = result_df[["open_price", "close_price"]].mean(axis=1)
                result_df["dvkpi_kpi"] = "AVG_PRICE"
                result_df["dvkpi_symbol_id"] = symbol_id
                result_df = result_df[["start_time","close_time", "dvkpi_kpi_value", "dvkpi_kpi", "dvkpi_symbol_id"]]
                result_df.columns = ["start_time","dvkpi_timestamp", "dvkpi_kpi_value", "dvkpi_kpi", "dvkpi_symbol_id"]
                result_df = result_df[["dvkpi_timestamp", "dvkpi_kpi", "dvkpi_kpi_value"]]
                #replace NaN with None
                result_df["dvkpi_kpi_value"] = result_df["dvkpi_kpi_value"].replace(np.nan,None)


            else:
                klines = Table("f_klines", MetaData(), autoload_with=engine)
                aggrtrades = Table("f_aggr_trades", MetaData(), autoload_with=engine)
                query = (
                    select(klines.c.start_time,klines.c.close_time, aggrtrades.c.quantity, aggrtrades.c.price)
                    .select_from(klines.outerjoin(aggrtrades,
                                                  aggrtrades.c.transact_time.between(klines.c.start_time, klines.c.close_time)))
                )

                result = connection.execute(query)
                result_df = pd.DataFrame(result)
                result_df = result_df.groupby(["start_time", "close_time"]).apply(lambda x: np.average(x.price, weights = x.quantity))
                result_df = result_df.reset_index()
                result_df["dvkpi_kpi"] = "AVG_PRICE"
                result_df["dvkpi_symbol_id"] = symbol_id
                result_df.columns = ["start_time","dvkpi_timestamp", "dvkpi_kpi_value", "dvkpi_kpi", "dvkpi_symbol_id"]
                result_df = result_df[["dvkpi_timestamp", "dvkpi_kpi", "dvkpi_kpi_value"]]
                #replace NaN with None
                result_df["dvkpi_kpi_value"] = result_df["dvkpi_kpi_value"].replace(np.nan,None)

            insert_df_to_table(result_df, db_url, "f_dvkpi")
        except Exception as e:
            print(f"Error calculating average prices for derived KPIs: {e}")



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