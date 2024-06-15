from download_data import load_historical_data_from_year
from transform import Transform

def run_all():
    load_historical_data_from_year(["etheur"], 2024)
    transformer = Transform()
    transformer.unzip("etheur")
    transformer.transform_and_load("etheur")

if __name__ == "__main__":
    run_all()