import pymongo
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["shopmart"]

# Function to import CSV file into MongoDB collection
def import_csv_to_mongo(collection_name, csv_file_path):
    # Read CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Convert DataFrame to a list of dictionaries
    records = df.to_dict(orient='records')
    
    # Insert records into the specified collection
    collection = db[collection_name]
    collection.insert_many(records)

# Import cleaned_transactions.csv into the 'transactions' collection
import_csv_to_mongo("transactions", "C:/Users/nic/Downloads/assignment/cleaned_transactions.csv")

# Import products.csv into the 'products' collection
import_csv_to_mongo("products", "C:/Users/nic/Downloads/assignment/products.csv")

print("Data import completed successfully.")