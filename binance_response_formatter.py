import pandas
from cross_cutting.utils import unix_to_datetime

def fix_klines_dataset(df, symbol_id):
    # Add Columns
    df['symbol_id'] = symbol_id
    df["start_time"] = df["start_time_numeric"].apply(unix_to_datetime)
    df["close_time"] = df["close_time_numeric"].apply(unix_to_datetime)

    df = df.astype({
        'open_price': 'float',
        'high_price': 'float',
        'low_price': 'float',
        'close_price': 'float',
        'volume': 'float',
        'number_of_trades': 'int'
    })

    return df

def fix_kpis_dataset(df: pandas.DataFrame, symbol_id: int) -> pandas.DataFrame:
    df['dvkpi_symbol_id'] = symbol_id
    df['dvkpi_timestamp'] = df['date']
    df.drop(columns=['date'], inplace=True)

    print(df)
    print(df.columns)
    df.columns = ['dvkpi_timestamp_numeric', 'dvkpi_kpi', 'dvkpi_kpi_value', 'dvkpi_symbol_id', 'dvkpi_timestamp']

    df = df.astype({
        'dvkpi_kpi_value': 'float',
        'dvkpi_kpi': 'str'
    })
    return df



