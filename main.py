import logging

from pandas.io.xml import preprocess_data

from src.data.data_processing import DataProcessing
from src.data.data_loader import DataLoader


def setup_loggin():
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)

def main():
    setup_loggin()
    logger = logging.getLogger(__name__)

    logger.info("Starting data loading import")
    data = DataProcessing(DataLoader("data/raw/").load_data())
    logger.info("Starting preprocessing")
    data.preprocess_data()

if __name__ == "__main__":
    main()