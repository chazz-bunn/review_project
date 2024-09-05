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

def change_type(x):
     if isinstance(x, set):
          return str(x)
     if x is None:
          return ''
     return x         

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

    # 1. How many distinct actors last names are there?
    actor_df.dropDuplicates(["last_name"]).agg({'last_name': 'count'}).show()
    #print("Number of unique last names: {}".format(actor_df.dropDuplicates(["last_name"]).count()))
    # 2. Which last names are not repeated?
    actor_df.groupBy(["last_name"]).count().filter("count = 1").select(["last_name"]).show()
    # 3. Which last names appear more than once?
    actor_df.groupBy(["last_name"]).count().filter("count > 1").select(["last_name"]).show()
    # 4. What is that average running time of all the films in the sakila DB?
    film_df.agg({'length': 'avg'}).show()
    # 5. What is the average running time of films by category?
    film_df.join(film_category_df, film_df.film_id == film_category_df.film_id)\
        .join(category_df, film_category_df.category_id == category_df.category_id)\
        .select("name", "length").groupBy("name").avg("length").show()

    cursor.close()
    cnx.close()

if __name__=='__main__':
    main()