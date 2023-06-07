from pymongo import MongoClient


def get_db():
    client = MongoClient('localhost', 27017)
    return client.mock_database


def call_batch(batch: list[dict]) -> None:

    for b in batch:
        b['function'](b['parameters'])
        print("---")