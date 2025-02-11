import pandas as pd
import kaggle
import os


class DataLoader:
    def __init__(self, dataset_path="data/", datasets=None):
        self.dataset_path = dataset_path
        self.available_datasets = datasets = {
            "category_tree": "category_tree.csv",
            "items_props_1": "item_properties_part1.csv",
            "items_props_2": "item_properties_part2.csv",
            "events": "events.csv",
        }
        self.datasets = datasets if datasets else list(self.available_datasets.keys())

    def download_data(self):
        kaggle.api.dataset_download_files(
            'retailrocket/ecommerce-dataset',
            path=self.dataset_path,
            unzip=True
        )

    def load_data(self):
        try:
            loaded_data = {}
            files_missing = False
            for dataset in self.datasets:
                file_path = os.path.join(self.dataset_path, self.available_datasets[dataset])
                if os.path.exists(file_path):
                    loaded_data[dataset] = pd.read_csv(file_path)
                else:
                    files_missing = True
                    print(f"File {file_path} does not exist.")
            if files_missing:
                self.download_data()
                return self.load_data()
            return loaded_data
        except Exception as e:
            print(f"Error loading data: {e}")
            self.download_data()
            return self.load_data()