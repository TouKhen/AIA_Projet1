from src.data.data_loader import DataLoader
import pandas as pd
import os


class DataProcessing:
    def __init__(self, data):
        data = [data] if isinstance(data, str) else data
        if 'items_prop_1' in data or 'items_prop_2' in data:
            data = list(set(data + ['items_prop_1', 'items_prop_2']))

        self.data = data
        self.data_loader = DataLoader(dataset_path="data/raw/", datasets=self.data)
        self.processed_path = os.path.join("data/processed/")


    def load_processed_data(self, dataset_name):
        file_path = os.path.join(self.processed_path, f"{dataset_name}_processed.csv")
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        return None


    def save_processed_data(self, dataset_name, data):
        file_path = os.path.join(self.processed_path, f"{dataset_name}_processed.csv")
        data.to_csv(file_path, index=False)


    def load_specific_data(self):
        data = self.data_loader.load_data()
        return data


    def preprocess_data(self):
        processed_data = {}

        for dataset in set(self.data):
            print(dataset)


    def preprocess_items(self):
        pass