import os

api_key_demo = "N17QNtKJF26Uc663zoxenuVyLO3KyWyiAOCOu4Z15NT6xqbrSFFfYi3jUdu7qvIq"
api_key_secret_demo = "wQaYNV1hHLPsdRMl8qB5Ks1e4zeRFXPkceasUgISSLRaehs4SThr7BtNcw0BP533"
db_conn = f"postgresql://db_user:pgpassword123@{os.getenv('DB_HOST', 'localhost')}/opa_db"
kpi_table = "f_dvkpi"
kpi_stream_table = "kpi_stream_table"
klines_stream_table = "f_klines_stream"
coin_to_trade = "ETH"
fiat_curr = "EUR"
aggregate_trades_stream_table = "f_aggr_trades_stream"
