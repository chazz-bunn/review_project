import mysql.connector
from pyspark.sql import SparkSession
import json

#Set up SparkSession and SparkContext
spark = SparkSession.builder.master("local").appName("Example1").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

#Login into mysql and sakila database, and setp cursor and connection objects
'''
Must have mysqllogininfo.json file with:
{
    "user":"[username]",
    "password":"[password]",
    "host":"[host address]"
}
'''
with open("mysqllogininfo.json") as jsonfile:
        login_dict = json.load(jsonfile)

cnx = mysql.connector.connect(
    user=login_dict["user"], 
    password=login_dict["password"], 
    host=login_dict["host"], 
    database="sakila"
)
cursor = cnx.cursor()

# The dataframes would fail to create when given set and None datatypes
# my programs replaces those datatypes using this function
def change_type(x):
     if isinstance(x, set):
          return str(x)
     if x is None:
          return ''
     return x         

# Function for converting a table to a dataframe
def table_to_df(table_name:str):
    cursor.execute(f"DESCRIBE {table_name};")
    results = cursor.fetchall()
    columns = [r[0] for r in results]
    cursor.execute(f"SELECT * FROM {table_name};")
    results = cursor.fetchall()
    results = [[change_type(j) for j in i] for i in results]
    return spark.createDataFrame(results, columns)

def main():
    #Load tables into dataframes
    actor_df = table_to_df("actor")
    film_df = table_to_df("film")
    film_category_df = table_to_df("film_category")
    category_df = table_to_df("category")

    ###QUERIES###
    # 1. How many distinct actors last names are there?
    q1_df = actor_df.dropDuplicates(["last_name"]).agg({'last_name': 'count'})
    print("\nHow many distinct actors last names are there?")
    q1_df.show(truncate = False)
    # 2. Which last names are not repeated?
    q2_df = actor_df.groupBy(["last_name"]).count().filter("count = 1").select(["last_name"])
    print("Which last names are not repeated?")
    q2_df.show(q2_df.count(), truncate = False)
    # 3. Which last names appear more than once?
    q3_df = actor_df.groupBy(["last_name"]).count().filter("count > 1").select(["last_name"])
    print("Which last names appear more than once?")
    q3_df.show(q3_df.count(), truncate = False)
    # 4. What is that average running time of all the films in the sakila DB?
    q4_df = film_df.agg({'length': 'avg'})
    print("What is that average running time of all the films in the sakila DB?")
    q4_df.show(truncate = False)
    # 5. What is the average running time of films by category?
    q5_df = film_df.join(film_category_df, film_df.film_id == film_category_df.film_id)\
        .join(category_df, film_category_df.category_id == category_df.category_id)\
        .selectExpr("name as category", "length").groupBy("category").avg("length")
    print("What is the average running time of films by category?")
    q5_df.show(q5_df.count(), truncate = False)


if __name__=='__main__':
    main()

sc.stop()
spark.stop()
cursor.close()
cnx.close()