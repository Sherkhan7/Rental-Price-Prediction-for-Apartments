"""
Cleaned Collection Schema (Conceptual)
{
  "_id": ObjectId,
  "price": Number,
  "rooms": Number,
  "area_sqm": Number,
  "district": String,
  "location": {
    "type": "Point",
    "coordinates": [longitude, latitude]
  },
  "price_per_sqm": Number,
  "created_at": ISODate
}
"""
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client["rental_db"]

raw = db["raw_listings"]
clean = db["apartments_clean"]

errors = []

# Transform and clean data
# Filter for "For rent" listings and valid lat/lon values
for doc in raw.find({"op_type": "For rent", "lat": {"$ne": float("nan")}, "lon": {"$ne": float("nan")}}):
    try:
        price = float(doc.get("price"))
        area = float(doc.get("area"))
        lat = float(doc.get("lat"))
        lon = float(doc.get("lon"))

        clean.insert_one({
            "row_id": doc.get("_id"),
            "price": price,
            "rooms": doc.get("rooms"),
            "area_sqm": area,
            "district": doc.get("district"),
            "location": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "price_per_sqm": price / area, # Calculate price per square meter
            "created_at": datetime.now()
        })
    except Exception:
        errors.append(
            {
                "doc_id": doc.get("_id"),
                "error": "transformation_failed"
            }
        )
        continue

print("Cleaned records:", clean.count_documents({}))
print("Transformation errors:", len(errors))
# Create geospatial index on location
clean.create_index([("location", "2dsphere")])
