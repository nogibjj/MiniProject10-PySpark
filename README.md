![Test Python Versions](https://github.com/nogibjj/IDS706-MiniProject5-Sqlite/actions/workflows/cicd.yml/badge.svg)
# IDS706 Mini Project10: GroceryDB CRUD Application

## Project Description

This project is a simple Python application that interacts with a SQLite database named `GroceryDB`. It provides basic CRUD functionality: Create, Read, Update, and Delete operations on the `GroceryDB` database. It is adpated from mini project5 while using Spark SQL.

## Queries Description & Results

### Query 1 - Read Operation
- This query performs read operation on the `GroceryDB` database. It selects all the rows from the `GroceryDB` table and displays the top 5 rows of the table.
```python
df.show(5)
```
### Result
![Query 1 Result](Results/Read.png "Query 1 Result")

### Query 2 - Update Operation
```python
df_updated = df.withColumn("count_products", 
                           when(df["general_name"] == "arabica coffee", 100)
                           .otherwise(df["count_products"]))
```
### Result
![Query 2 Result](Results/update.png "Query 2 Result")

### Query 3 - Delete Operation
```python
df_updated = df_updated.filter(df_updated["general_name"] != "arabica coffee")
```
### Result
![Query 3 Result](Results/delete.png "Query 3 Result")

## How to Run
```bash
python3 spark.py
```
## Thoughts about Spark SQL and other tools
| Feature | ORM | Spark | Pandas |
|---|---|---|---|
| **Focus** | Object-relational mapping | Distributed computing | Data manipulation and analysis |
| **Use cases** | Traditional relational databases | Big data applications | Data analysis |
| **Data types** | Objects | Structured data | Structured data, semi-structured data, unstructured data |
| **Programming languages** | Supports multiple languages | Supports multiple languages | Python |
| **Advantages** | High development efficiency, easy to use | Efficient for processing large datasets, scalable | Powerful data analysis and visualization features |
| **Disadvantages** | Not suitable for large-scale data processing | Steep learning curve | Not suitable for traditional relational databases |
| **Examples** | Developing a small-scale application that needs to interact with a relational database | Processing large datasets across multiple machines | Performing data manipulation and analysis on smaller datasets |

