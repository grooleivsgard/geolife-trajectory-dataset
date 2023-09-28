from kaggle.api.kaggle_api_extended import KaggleApi

# Initialize the Kaggle API client
api = KaggleApi()
api.authenticate()

# Download dataset
# The dataset is in the format `username/dataset`
# For example, to download the Titanic dataset use: 'heptapod/titanic'
api.dataset_download_files('arashnic/microsoft-geolife-gps-trajectory-dataset', path='./geolife_ds', unzip=True)
