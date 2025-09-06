# Generate a listings.csv.gz file in the current DAG directory with the required header and IDs
import gzip
import csv
import os

listings_file_path = os.path.join(os.path.dirname('/tmp/data/listings/2025-09/'), "listings.csv.gz")
listings_header = ["id", "name", "price"]
listings_rows = [
    {"id": 101, "name": "Modern Sofa", "price": 799},
    {"id": 102, "name": "Cordless Vacuum Cleaner", "price": 159},
    {"id": 103, "name": "Ergonomic Office Chair", "price": 229},
    {"id": 104, "name": "Stainless Steel Cookware Set", "price": 119},
    {"id": 105, "name": "Smart LED TV 50-inch", "price": 499},
    {"id": 106, "name": "Multi-Tool Power Drill", "price": 89},
    {"id": 107, "name": "Queen Size Bed Frame", "price": 349},
    {"id": 108, "name": "Air Purifier", "price": 179},
]
# Write the listings.csv.gz file if it does not exist
if not os.path.exists(listings_file_path):
    with gzip.open(listings_file_path, "wt", newline="") as gzfile:
        writer = csv.DictWriter(gzfile, fieldnames=listings_header)
        writer.writeheader()
        for row in listings_rows:
            writer.writerow(row)



