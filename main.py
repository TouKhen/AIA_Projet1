import logging
from src.data.data_loader import DataLoader


def setup_loggin():
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)

def main():
    setup_loggin()
    logger = logging.getLogger(__name__)

    logger.info("Starting data loading import")
    data_loader = DataLoader(dataset_path="data/raw/")
    events, category_tree, items_prop_1, items_prop_2 = data_loader.load_data()
    logger.info("Data loaded successfully")

    logger.info("Starting preprocessing")

    print(events.head())

if __name__ == "__main__":
    main()