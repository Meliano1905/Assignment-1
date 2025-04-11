import pandas as pd

# Load transactions.csv
transactions = pd.read_csv('transactions.csv')

# Load products.csv
products = pd.read_csv('products.csv')

# Define the default date
default_date = '01/05/2023'

# Standardize date formats in transactions
transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'], errors='coerce', format='%d/%m/%Y')
transactions['transaction_date'].fillna(pd.to_datetime(default_date, format='%d/%m/%Y'), inplace=True)
transactions['transaction_date'] = transactions['transaction_date'].dt.strftime('%Y-%m-%d')

# Identify and handle missing values in transactions
print("Missing values in transactions:")
print(transactions.isnull().sum())

# Fill missing values in the 'price' column with the average price
transactions['price'].fillna(transactions['price'].mean(), inplace=True)

# Fill missing values in the 'quantity' column with the median quantity
transactions['quantity'].fillna(transactions['quantity'].median(), inplace=True)

# Remove duplicate entries
transactions.drop_duplicates(subset='transaction_id', inplace=True)

# Identify and handle missing values in products
print("Missing values in products:")
print(products.isnull().sum())

# Fill missing values in the 'price' column with the average price
products['price'].fillna(products['price'].mean(), inplace=True)

# Standardize text entries
products['product_name'] = products['product_name'].str.title()
products['category'] = products['category'].str.title()

# Ensure numeric values are correctly typed
transactions['price'] = pd.to_numeric(transactions['price'], errors='coerce')
transactions['quantity'] = pd.to_numeric(transactions['quantity'], errors='coerce')

# Save cleaned data
transactions.to_csv('cleaned_transactions.csv', index=False)
products.to_csv('cleaned_products.csv', index=False)

print("Data cleaning and standardization completed successfully.")