import mysql.connector
from pyspark.sql import SparkSession
import json

#Set up SparkSession and SparkContext
spark = SparkSession.builder.master("local").appName("Example1").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

### MUST have file mysqllogininfo.json file with the following:
'''
{
    "user":"[username]",
    "password":"[password]",
    "host":"[host address]"
}
'''
with open("mysqllogininfo.json") as jsonfile:
     login_dict = json.load(jsonfile)

# Set up cursor and connection objects
cnx = mysql.connector.connect(
     user=login_dict["user"], 
     password=login_dict["password"], 
     host=login_dict["host"], 
     database="sakila"
)
cursor = cnx.cursor()

# The dataframes would fail to create when given 'set' or 'None' datatypes
# this function is used to change those types to acceptable types
def change_type(x):
     if isinstance(x, set):
          return list(x)
     if x is None:
          return ''
     return x    

# Function for converting a table to a dataframe
def table_to_df(table_name:str):
    cursor.execute(f"DESCRIBE {table_name};")
    descriptor = cursor.fetchall()
    columns = [column[0] for column in descriptor]
    cursor.execute(f"SELECT * FROM {table_name};")
    results = []
    while True:
         try:
              results.append([change_type(f) for f in cursor.fetchone()])
         except TypeError:
              break
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