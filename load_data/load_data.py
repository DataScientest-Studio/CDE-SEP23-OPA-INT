from datetime import datetime

import pandas as pd

import download_data as dd
from utility import convert_to_date_object


def load_historical_data_from_year(symbols, year):
    if not year:
        period = convert_to_date_object(datetime.today().strftime('%Y-%m-%d')) - convert_to_date_object(
            f"01-01-{year}")
        dates = pd.date_range(end=datetime.today(), periods=period.days + 1).to_pydatetime().tolist()
        dates = [date.strftime("%Y-%m-%d") for date in dates]

    dd.download_daily_klines(trading_type="spot", symbols=symbols, num_symbols=len(symbols), dates=dates)


load_historical_data_from_year(["etheur"], 2024)
