import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["rental_db"]

raw = db["raw_listings"]

df = pd.read_csv("../data/1/riga_re.csv")

records = df.to_dict(orient="records")

for r in records:
    r["source"] = "kaggle_riga"
    raw.insert_one(r)

print("Raw data ingested:", raw.count_documents({}))
