import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["shopmart"]

# Query 1: Top 5 Products by Total Sales
def top_5_products_by_sales():
    pipeline = [
        {
            "$lookup": {
                "from": "products",
                "localField": "product_id",
                "foreignField": "product_id",
                "as": "product_details"
            }
        },
        {
            "$unwind": "$product_details"
        },
        {
            "$group": {
                "_id": "$product_details.product_id",
                "total_sales": { "$sum": { "$multiply": ["$quantity", "$price"] } },
                "product_name": { "$first": "$product_details.name" }
            }
        },
        {
            "$sort": { "total_sales": -1 }
        },
        {
            "$limit": 5
        }
    ]
    results = db.transactions.aggregate(pipeline)
    print("Top 5 Products by Total Sales:")
    for result in results:
        print(result)

# Query 2: Product Category with Highest Sales Volume
def highest_sales_volume_category():
    pipeline = [
        {
            "$lookup": {
                "from": "products",
                "localField": "product_id",
                "foreignField": "product_id",
                "as": "product_details"
            }
        },
        {
            "$unwind": "$product_details"
        },
        {
            "$group": {
                "_id": "$product_details.category",
                "total_quantity": { "$sum": "$quantity" }
            }
        },
        {
            "$sort": { "total_quantity": -1 }
        },
        {
            "$limit": 1
        }
    ]
    results = db.transactions.aggregate(pipeline)
    print("Product Category with Highest Sales Volume:")
    for result in results:
        print(result)

# Query 3: Average Transaction Amount
def average_transaction_amount():
    pipeline = [
        {
            "$group": {
                "_id": None,
                "average_transaction_amount": { "$avg": { "$multiply": ["$quantity", "$price"] } }
            }
        }
    ]
    results = db.transactions.aggregate(pipeline)
    print("Average Transaction Amount:")
    for result in results:
        print(result)

# Run the queries
if __name__ == "__main__":
    top_5_products_by_sales()
    highest_sales_volume_category()
    average_transaction_amount()