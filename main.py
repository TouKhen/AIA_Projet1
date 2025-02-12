import logging
from src.data.data_loader import DataLoader


def setup_loggin():
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)

def main():
    setup_loggin()
    logger = logging.getLogger(__name__)

    logger.info("Starting data loading import")
    data = DataLoader("data/raw/").load_data()
    print(data['event'].head())
    logger.info("Starting preprocessing")

if __name__ == "__main__":
    main()