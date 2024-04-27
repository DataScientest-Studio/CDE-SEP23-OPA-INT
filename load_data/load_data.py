from datetime import datetime

import pandas as pd

import download_data as dd
from utility import convert_to_date_object


def load_historical_data_from_year(symbols, year):
    if not year:
        year = 2024

    period = convert_to_date_object(datetime.today().strftime('%Y-%m-%d')) - convert_to_date_object(
            f"{year}-01-01")
    dates = pd.date_range(end=datetime.today(), periods=period.days + 1).to_pydatetime().tolist()
    dates = [date.strftime("%Y-%m-%d") for date in dates]

    dd.download_daily_klines(trading_type="spot",
                             symbols=symbols,
                             num_symbols=len(symbols),
                             intervals=["1m"],
                             start_date=None,
                             end_date=None,
                             folder=None,
                             checksum=0,
                             dates=dates)

