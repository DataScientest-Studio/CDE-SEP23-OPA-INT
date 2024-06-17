
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, insert, select


def insert_df_to_table(df, db_url, table_name):
    data = df.to_dict(orient='records')
    insertTableData(db_url, table_name, data)


def insertTableData(db_url, table_name, data):
    engine = create_engine(db_url, echo=False)
    # print(data)
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
def create_derived_kpis(db_url, symbol_id, df_klines):
    engine = create_engine(db_url, echo=False)

    # Calculate average prices first
    with engine.connect() as connection:
        try:
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
            df['dvkpi_timestamp'] = pd.to_datetime(df['dvkpi_timestamp'], format='%Y-%m-%d %H:%M:%S', utc=True)
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


def load_symbol_id(db_url, symbol):
    engine = create_engine(db_url)
    with engine.connect() as connection:
        try:
            symbols = Table("d_symbols", MetaData(), autoload_with=engine)
            stmt = select(symbols).where(symbols.c.symbol == symbol)
            result = connection.execute(stmt)
            symbol_id = pd.DataFrame(result).iloc[0, 0]
        except Exception as e:
            print(f"Error retrieving data: {e}")
    return symbol_id


def create_derived_kpis_from_pred(df_pred_klines, symbol_id):
    try:
        result_df = pd.DataFrame(df_pred_klines)
        result_df = result_df.reset_index()
        result_df.columns = ["dvkpi_timestamp", "dvkpi_kpi_value"]
        result_df["dvkpi_kpi"] = "AVG_PRICE"
        result_df["dvkpi_symbol_id"] = symbol_id
        result_df = result_df[["dvkpi_timestamp", "dvkpi_kpi", "dvkpi_kpi_value", "dvkpi_symbol_id"]]
        # replace NaN with None
        result_df["dvkpi_kpi_value"] = result_df["dvkpi_kpi_value"].replace(np.nan, None)
        return result_df
    except Exception as e:
        print(f"Error calculating average prices for derived KPIs for prediction: {e}")


def load_data_from_db_table(db_url, table_name, symbol_id=1):
    df = pd.DataFrame(get_data_from_db_table(db_url, table_name, symbol_id))
    return df
