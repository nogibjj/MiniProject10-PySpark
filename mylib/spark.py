from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CSV Data Processing") \
    .getOrCreate()

# Path to the CSV file
csv_file_path = 'data/GroceryDB_IgFPro.csv'  # Replace with the path to your CSV file

# Read the CSV file into a DataFrame
df = spark.read.option("header", "true").csv(csv_file_path)

# Show DataFrame schema
df.printSchema()

# Show the first 3 rows of the DataFrame
df.show(3)

# Stop the Spark session
spark.stop()
