from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

def load(dataset="data/GroceryDB_IgFPro.csv"):
    """Transforms and Loads data into the local SQLite3 database using Spark"""
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("GroceryDB_ETL") \
        .config("spark.driver.extraClassPath", "path_to_sqlite_jdbc") \
        .getOrCreate()

    # Define the schema of the CSV file
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("general_name", StringType(), True),
        StructField("count_products", IntegerType(), True),
        StructField("ingred_FPro", StringType(), True),
        StructField("avg_FPro_products", StringType(), True),
        StructField("avg_distance_root", StringType(), True),
        StructField("ingred_normalization_term", StringType(), True),
        StructField("semantic_tree_name", StringType(), True),
        StructField("semantic_tree_node", StringType(), True)
    ])

    # Read the CSV file into a DataFrame
    df = spark.read.csv(dataset, schema=schema, header=True)

    # Show the directory for debugging purposes
    print(os.getcwd())

    # Load the DataFrame into the SQLite database
    # Note: The mode can be "overwrite", "append", "ignore", "error" (default).
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:sqlite:GroceryDB.db") \
      .option("dbtable", "GroceryDB") \
      .option("driver", "org.sqlite.JDBC") \
      .mode("overwrite") \
      .save()

    spark.stop()
    return "GroceryDB.db"

# Example usage
load()
