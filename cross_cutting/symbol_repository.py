from sqlalchemy import create_engine, insert, Table, MetaData, select


class SymbolRepository:
    def __init__(self, db_connection_string):
        self.engine = create_engine(db_connection_string)
        self.symbol_table = Table("d_symbols", MetaData(), autoload_with=self.engine)


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

