import os

from sqlalchemy import MetaData, create_engine, Column, Table, TIMESTAMP, Numeric, ForeignKey, Integer, String, \
    PrimaryKeyConstraint, select, insert

db_conn = os.getenv("DB_CONN", "postgresql://db_user:pgpassword123@localhost/opa_db")
engine = create_engine(db_conn)

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

dvkpis_table = Table('f_dvkpi', metadata,
                     Column('dvkpi_timestamp', TIMESTAMP(timezone=True)),
                     Column('dvkpi_timestamp_numeric', Numeric),
                     Column('dvkpi_symbol_id', Integer, ForeignKey('d_symbols.symbol_id')),
                     Column('dvkpi_kpi', String),
                     Column('dvkpi_kpi_value', Numeric),
                     PrimaryKeyConstraint('dvkpi_timestamp', 'dvkpi_symbol_id', 'dvkpi_kpi', name='pk_kpis')
                     )

dvkpis_stream_table = Table('f_dvkpi_stream', metadata,
                            Column('dvkpi_timestamp', TIMESTAMP(timezone=True)),
                            Column('dvkpi_timestamp_numeric', Numeric),
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


def create_db():
    try:
        metadata.create_all(engine, checkfirst=True)
        print("created all db")
    except Exception as e:
        print(f"Error creating tables: {e}")


def insert_initial_data():
    with engine.connect() as connection:
        stmt = select(symbols_table).where(symbols_table.c.symbol_id == 1)
        result = connection.execute(stmt).fetchone()
        if not result:
            stmt = insert(symbols_table).values(symbol_id=1, symbol='ETHEUR')
            connection.execute(stmt)
            connection.commit()


create_db()
insert_initial_data()
