from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["rental_db"]

clean = db["apartments_clean"]

center = {
    "type": "Point",
    "coordinates": [24.6032, 56.8796] # Riga city center (longitude, latitude)
}

nearby = clean.find({
    "location": {
        "$near": {  # Use $near for geospatial query and $near requires a 2dsphere indexing
            "$geometry": center,  # Riga city center
            "$maxDistance": 3000  # 3 kilometers
        }
    }
})

nearby_list = list(nearby)

print("Apartments within 3 km of Riga city center:", len(nearby_list))
for apt in nearby_list:
    print(
        f"ID: {apt['_id']}, Price: {apt['price']}, Area: {apt['area_sqm']} sqm, Location: {apt['location']['coordinates']}")