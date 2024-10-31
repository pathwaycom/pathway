import sys
import time

from pymongo import MongoClient

if __name__ == "__main__":
    client = MongoClient("mongodb://mongodb:27017/?replicaSet=rs0")
    db = client["test_database"]
    collection = db["values"]
    for i in range(1, 1001):
        collection.insert_one({"value": i})
        print("Insert:", i, file=sys.stderr)
        time.sleep(0.5)
