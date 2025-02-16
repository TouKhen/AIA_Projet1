import math

import numpy as np
from src.data.data_loader import DataLoader
import pandas as pd
import os


class DataProcessing:
    def __init__(self, data):
        self.data = data
        self.data_loader = DataLoader(dataset_path="data/raw/", datasets=self.data)
        self.processed_path = os.path.join("data/processed/")


    def load_processed_data(self, dataset_name):
        file_path = os.path.join(self.processed_path, f"{dataset_name}_processed.csv")
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        return None


    def save_processed_data(self, dataset_name, data):
        file_path = os.path.join(self.processed_path, f"{dataset_name}_processed.parquet")
        data.to_parquet(file_path)


    def load_specific_data(self):
        data = self.data_loader.load_data()
        return data


    def preprocess_data(self):
        processed_data = {}

        processed_map = {
            "events": self.preprocess_events(),
            "category_tree": self.preprocess_category_tree()
        }

        data = self.load_specific_data()

        for dataset in set(self.data):
            if dataset not in processed_data:
                if not dataset == 'items_props_1' and not dataset == 'items_props_2':
                    processed_data[dataset] = processed_map[dataset]

        processed_data['items_prop'] = self.preprocess_items()

        for dataset in set(processed_data):
            self.save_processed_data(dataset, processed_data[dataset])

        return None


    def preprocess_items(self):
        data = self.load_specific_data()
        # Concat items_props_1 and items_props_1 to have only one file
        new_data = pd.concat([data['items_props_1'], data['items_props_2']], ignore_index=True)

        # Add datetime and day, month and year to data
        new_data["datetime"] = pd.to_datetime(new_data["timestamp"], unit="ms")
        new_data['day'] = new_data['datetime'].dt.day_name()
        new_data['month'] = new_data['datetime'].dt.month_name()
        new_data['year'] = new_data['datetime'].dt.year

        return new_data


    def preprocess_events(self):
        data = self.load_specific_data()['events']

        # =============================
        # Events Data
        # =============================
        print("\n=============================")
        print("Events Data")
        print("=============================\n")

        # Types and numbers of events
        view_events = data[data['event'] == 'view']
        addtocart_events = data[data['event'] == 'addtocart']
        transaction_events = data[data['event'] == 'transaction']

        print(f"Number of views: {len(view_events)}")
        print(f"Number of addtocart: {len(addtocart_events)}")
        print(f"Number of transaction: {len(transaction_events)}")

        unique_users = data['visitorid'].unique()
        users_addtocart = addtocart_events['visitorid'].unique()
        users_transaction = transaction_events['visitorid'].unique()

        print(f"Number of unique users: {len(unique_users)}")
        print(f"Number of unique users that added to cart an item: {len(users_addtocart)}")
        print(f"Number of unique users that made a transaction: {len(users_transaction)}")

        # Convert timestamp to datetime
        print("\n=====")
        print("Converting timestamp to datetime")
        print("=====\n")

        data["datetime"] = pd.to_datetime(data["timestamp"], unit="ms")

        # Add day, month and year
        print("Adding day, month and year to new columns")

        data['day'] = data['datetime'].dt.day_name()
        data['month'] = data['datetime'].dt.month_name()
        data['year'] = data['datetime'].dt.year

        # Start and end date of data
        print(f"Data starting date {data['datetime'].min()}")
        print(f"Data ending date {data['datetime'].max()}")
        print(f"Length of data recording {data['datetime'].max() - data['datetime'].min()}")

        # Add true or false value if the event has a transaction id
        print("\n=====")
        print("Conversion rate between events")
        print("=====\n")
        data['hasTransaction'] = np.where(data['transactionid'].isnull(), False, True)

        # Conversion rate from view to addtocart
        add_to_cart_count = len(users_addtocart)

        print(f"Conversion rate from view to addtocart : {np.round(add_to_cart_count / len(view_events) * 100, 2)}%")

        # Conversion rate of users who bought without adding to cart
        view_only_conversions = view_events[
            (view_events['visitorid'].isin(users_addtocart) == False) & (
                        view_events['visitorid'].isin(
                            users_transaction) == True)]['visitorid']
        view_only_users = data[~data['visitorid'].isin(users_addtocart)]

        view_only_total_users = len(view_only_users)

        view_only_conversion_rate = len(
            view_only_conversions) / view_only_total_users * 100
        print(
            f"Conversion rate of users who bought but didn't addtocart : {view_only_conversion_rate:.3f}%")

        # Conversion rate from addtocart to transaction
        transaction_count = transaction_events[
            transaction_events["visitorid"].isin(users_addtocart)]['visitorid']
        print(
            f"Conversion rate from addtocart to transaction : {np.round(len(transaction_count) / add_to_cart_count * 100, 2)}%")

        return data


    def preprocess_category_tree(self):
        data = self.load_specific_data()['category_tree']

        primary_categories = data[data['parentid'].isnull()]['categoryid']
        tier2_cat = data[data['parentid'].isin(primary_categories)]['categoryid']
        tier3_cat = data[data['parentid'].isin(tier2_cat)]['categoryid']
        tier4_cat = data[data['parentid'].isin(tier3_cat)]['categoryid']
        tier5_cat = data[data['parentid'].isin(tier4_cat)]['categoryid']
        tier6_cat = data[data['parentid'].isin(tier5_cat)]['categoryid']

        # Add category level based on child/parent
        data['category_level'] = [
            "level_1" if x in primary_categories.values
            else "level_2" if x in tier2_cat.values
            else "level_3" if x in tier3_cat.values
            else "level_4" if x in tier4_cat.values
            else "level_5" if x in tier5_cat.values
            else "level_6" if x in tier6_cat.values
            else None for x in data['categoryid']
        ]

        print("\n=============================")
        print("Category Tree Data")
        print("=============================\n")

        print("\nPrimary categories")
        print(len(primary_categories))

        print("\nTier 2 categories")
        print(len(tier2_cat))

        print("\nTier 3 categories")
        print(len(tier3_cat))

        print("\nTier 4 categories")
        print(len(tier4_cat))

        print("\nTier 5 categories")
        print(len(tier5_cat))

        print("\nTier 6 categories")
        print(len(tier6_cat))

        return data