from download_data import load_historical_data_from_year
# from transform import Transform
import etl_settings
def run_all():
    
    all_historical_files_loaded = False
    while all_historical_files_loaded == False:
        all_historical_files_loaded = load_historical_data_from_year(["etheur"], etl_settings.download_year)

    # transformer = Transform()
    # transformer.unzip("etheur")
    # transformer.transform_and_load("etheur")

if __name__ == "__main__":
    run_all()