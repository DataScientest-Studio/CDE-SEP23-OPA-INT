from sqlalchemy import create_engine, insert, Table, MetaData, select, and_
import pandas as pd
import binance_response_formatter as brf

class SymbolRepository:
    def __init__(self):
        db_connection_string = "postgresql://db_user:pgpassword123@localhost/opa_db"
        self.engine = create_engine(db_connection_string)
        self.symbol_table = Table("d_symbols", MetaData(), autoload_with=self.engine)
        self.kline_table = Table("f_klines", MetaData(), autoload_with=self.engine)
        self.kpi_table = Table("f_dvkpi", MetaData(), autoload_with=self.engine)

    def add_symbol(self, symbol_name):
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                stmt = insert(self.symbol_table).values(symbol=symbol_name).returning(self.symbol_table.c.symbol_id)
                result = connection.execute(stmt)
                trans.commit()
                return result.fetchone()[0]
            except Exception as e:
                trans.rollback()
                print(f"Error adding symbol: ${symbol_name}", e)

    def get_symbol_id(self, symbol_name):
        select_stmt = select(self.symbol_table.c.symbol_id).where(self.symbol_table.c.symbol == symbol_name)
        with self.engine.connect() as connection:
            result = connection.execute(select_stmt)

        row = result.fetchone()
        if row:
            return row[0]  # Return the symbol_id
        return None

    def get_last_kline_timestamp(self, symbol):
        select_stmt = (
            select(self.kline_table.c.start_time_numeric)
            .join(self.symbol_table, self.kline_table.c.symbol_id == self.symbol_table.c.symbol_id)
            .where(self.symbol_table.c.symbol == symbol)
            .order_by(self.kline_table.c.start_time_numeric.desc())
            .limit(1)
        )

        try:
            with self.engine.connect() as connection:
                result = connection.execute(select_stmt)

            row = result.fetchone()
            if row:
                return row[0]
        except Exception as e:
            print(f"Error getting last kline timestamp for symbol_id: {symbol}", e)

    def get_last_kpis(self, symbol, last_time_stamp):
        select_stmt = (
            select(self.kpi_table.c.dvkpi_timestamp_numeric, self.kpi_table.c.dvkpi_kpi,
                   self.kpi_table.c.dvkpi_kpi_value, self.kpi_table.c.dvkpi_symbol_id)
            .join(self.symbol_table, self.kpi_table.c.dvkpi_symbol_id == self.symbol_table.c.symbol_id)
            .where(
                and_(
                    self.symbol_table.c.symbol == symbol
                )
            )
            .order_by(self.kpi_table.c.dvkpi_timestamp_numeric.desc())
            .limit(2500)
        )

        try:
            with self.engine.connect() as connection:
                result = connection.execute(select_stmt)

            rows = result.fetchall()
            if rows:
                dict_list = [row._asdict() for row in rows]
                return pd.DataFrame(dict_list)
        except Exception as e:
            print(f"Error getting last kpi timestamp for symbol_id: {symbol}", e)

