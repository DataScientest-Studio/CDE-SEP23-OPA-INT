from datetime import datetime


def unix_to_datetime(unix_timestamp):
    return datetime.fromtimestamp(unix_timestamp / 1000)