import requests
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

def extract(url="https://raw.githubusercontent.com/"
                "Barabasi-Lab/GroceryDB/main/data/GroceryDB_IgFPro.csv",
            file_path="data/GroceryDB_IgFPro.csv"):
    """Extract a url to a file path."""
    # Ensure the data directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Download the data
    with requests.get(url) as r:
        with open(file_path, 'wb') as f:
            f.write(r.content)
    return file_path

# Call the extract function to download the data
csv_file_path = extract()

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CSV Data Processing with Queries") \
    .getOrCreate()

# Read the data into a DataFrame
df = spark.read.option("header", "true").csv(csv_file_path)

# Renaming the column to replace space with underscore
df = df.withColumnRenamed("general name", "general_name")

# Query 1: Show the top 5 rows of the DataFrame
df.show(5)

# Query 2: Update 'count_products' for 'arabica coffee' to 100
df_updated = df.withColumn("count_products", 
                           when(df["general_name"] == "arabica coffee", 100)
                           .otherwise(df["count_products"]))
# Show the updated DataFrame
df_updated.filter(df_updated["general_name"] == "arabica coffee").show()

# Query 3: Delete the row containing 'arabica coffee in the GroceryDB table'
df_updated = df_updated.filter(df_updated["general_name"] != "arabica coffee")
# Show top 5 rows of the updated DataFrame
df_updated.show(5)

# Stop the Spark session
spark.stop()
