from load_data.download_data import load_historical_data_from_year
from load_data.transform import Transform

def run_all():
    load_historical_data_from_year(["etheur"], 2024)
    transformer = Transform()
    transformer.unzip("etheur")
    transformer.transform_and_load("etheur")

def download():
    load_historical_data_from_year(["etheur"], 2024)

def trasnform():
    transformer = Transform()
    transformer.unzip("etheur")
    transformer.transform_and_load("etheur")