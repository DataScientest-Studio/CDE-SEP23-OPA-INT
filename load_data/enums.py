from datetime import *

YEARS = ['2021', '2022', '2023', '2024']
INTERVALS = ["1m"]
DAILY_INTERVALS = ["1m"]
TRADING_TYPE = ["spot"]
MONTHS = list(range(1,13))
PERIOD_START_DATE = '2024-01-01'
BASE_URL = 'https://data.binance.vision/'
START_DATE = date(int(YEARS[0]), MONTHS[0], 1)
END_DATE = datetime.now() - timedelta(days=1)