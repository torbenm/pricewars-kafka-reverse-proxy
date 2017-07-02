import csv
import json
import threading
import time
import pandas as pd
from random import randint
from datetime import datetime, timedelta

rows_size = 100
min_merchants = 1
max_merchants = 10
max_quality = 5
min_price = 1
max_price = 10
selling_probability = 10
max_shipping_time_standard = 5


# @staticmethod
# def generate_merchant_config():

def create_marketsituation_csv():
    count = 0
    filename = int(time.time())
    filepath = 'data/' + str(filename) + '.csv'
    rows = []
    while count < rows_size:
        rows.append(create_row(count))
        count += 1
        # print(count)
    write_to_csv(rows, filepath)
    return {'url': filepath}


def write_to_csv(content, filepath):
    df = pd.DataFrame(content)
    df.to_csv(filepath, index=False)


def create_row(count):
    row = {}
    row["timestamp"] = (datetime.now() + timedelta(hours=count)).isoformat()
    row["offer_id"] = 0
    row["product_id"] = randint(1, 9)
    row["quality"] = randint(1, 9)
    row["uid"] = int(str(row["product_id"]) + str(row["quality"]))
    row["merchant_id"] = randint(min_merchants, max_merchants)
    row["amount"] = 1
    row["price"] = randint(1, 9)
    row["shipping_time_standard"] = randint(1, 6)
    row["shipping_time_prime"] = 1
    row["prime"] = False
    # print(merchant)
    return row


def sell_yn():
    if selling_probability > randint(0, 100):
        return True
    else:
        return False
