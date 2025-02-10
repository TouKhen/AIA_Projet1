import pandas as pd
import kaggle
import os


class DataLoader:
    def __init__(self, dataset_path="data/"):
        self.dataset_path = dataset_path

    def download_data(self):
        kaggle.api.dataset_download_files(
            'retailrocket/ecommerce-dataset',
            path=self.dataset_path,
            unzip=True
        )

    def load_data(self):
        try:
            events = pd.read_csv(os.path.join(self.dataset_path + "events.csv"))
            category_tree = pd.read_csv(os.path.join(self.dataset_path + "category_tree.csv"))
            items_prop_1 = pd.read_csv(os.path.join(self.dataset_path + "item_properties_part1.csv"))
            items_prop_2 = pd.read_csv(os.path.join(self.dataset_path + "item_properties_part2.csv"))
            return events, category_tree, items_prop_1, items_prop_2
        except Exception as e:
            print(f"Error loading data: {e}")
            self.download_data()
            events = pd.read_csv(os.path.join(self.dataset_path + "events.csv"))
            category_tree = pd.read_csv(os.path.join(self.dataset_path + "category_tree.csv"))
            items_prop_1 = pd.read_csv(os.path.join(self.dataset_path + "item_properties_part1.csv"))
            items_prop_2 = pd.read_csv(os.path.join(self.dataset_path + "item_properties_part2.csv"))
            return events, category_tree, items_prop_1, items_prop_2