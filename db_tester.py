from sqlalchemy import create_engine, Table, MetaData, select
from settings import Settings

def getFirstTenRows(db_url, table_name):
    engine = create_engine(db_url)

    with engine.connect() as connection:
        try:
            # Reflecting the table from the database
            table = Table(table_name, MetaData(), autoload_with=engine)

            # Building a SELECT statement to fetch the first 10 rows
            stmt = select(table).limit(10)

            # Executing the query
            result = connection.execute(stmt)

            # Fetching the results
            rows = result.fetchall()

            # Print or process the rows as needed
            for row in rows:
                print(row)

        except Exception as e:
            print(f"Error retrieving data: {e}")


print('Klines_table')
getFirstTenRows(Settings.get_setting("db_conn"), Settings.get_setting('klines_table'))

print('aggregate_trades_table')
getFirstTenRows(Settings.get_setting("db_conn"), Settings.get_setting('aggregate_trades_table'))

print('symbols_table')
getFirstTenRows(Settings.get_setting("db_conn"), Settings.get_setting('symbols_table'))