from kaggle.api.kaggle_api_extended import KaggleApi

# Initialize the Kaggle API client
api = KaggleApi()
api.authenticate()

# Download dataset
api.dataset_download_files('arashnic/microsoft-geolife-gps-trajectory-dataset', path='./geolife_ds', unzip=True)
