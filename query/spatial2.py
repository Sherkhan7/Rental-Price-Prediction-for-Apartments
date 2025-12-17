from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["rental_db"]

clean = db["apartments_clean"]

pipeline = [
    {
        "$group": {
            "_id": "$district",  # Group by district
            "avg_ppsqm": {"$avg": "$price_per_sqm"}  # Calculate average price per square meter
        }
    },
    {"$sort": {"avg_ppsqm": -1}}  # Sort by average price per square meter descending
]

for r in clean.aggregate(pipeline):
    print(r)
