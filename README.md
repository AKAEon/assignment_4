# assignment_4
u.user is demographic information about the user; this is a tab-delimited list. The user IDs are the IDs used in the u.data dataset.

u.data -- The full u dataset, 100,000 ratings of 1,682 items by 943 users. Each user rated at least 20 movies. Users and items are numbered consecutively starting at 1. The data is randomly sorted. This is a tab-delimited list containing user id | item id | rating | timestamp. Timestamp is the number of unix seconds since January 1, 1970 UTC.

For u.item. The last 19 fields are genres, 1 means the movie belongs to that genre, 0 means it does not; a movie can belong to multiple genres at the same time. The movie ID is the ID used in the u.data dataset.


First, run ```sudo service cassandra start``` to start Cassandra.
Run ```sqlsh``` to enter the sqlsh shell.

Create database
```
CREATE KEYSPACE movielens WITH replication = {‘class’: ‘SimpleStrategy’, ‘replication_factor’:’1’} AND durable_writes = true;
```
Create three tables, users and data, and define column names.
```
cqlsh> USE movielens;
cqlsh:movielens> CREATE TABLE users(user_id int, age int, gender text, occupation text, zip text, PRIMARY KEY(user_id));
cqlsh:movielens> CREATE TABLE datas(user_id int, movie_id int, rating int, timstamp int, PRIMARY KEY(user_id));
cqlsh:movielens> CREATE TABLE items (movie_id int,movie_title text,release_date text,video_release_date text,IMDb_URL text,unknown int,Action int,Adventure int,Animation int,Childrens int,Comedy int,Crime int,Documentary int,Drama int, Fantasy int, FilmNoir int, Horror int, Musical int, Mystery int, Romance int, SciFi int, Thriller int, War int, Western int,  PRIMARY KEY(movie_id));

cqlsh:movielens> exit
```
Next, create a Python script in maria_dev using ```vi assignment4.py```.
The code to complete the task is as follows:
```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])

def datasInput(line):
    fields = line.split('\t')
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating = int(fields[2]),timstamp=(fields[3]))


if __name__ == "__main__":
    # Create a sparksession
    spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")
    lines2 = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

   # Convert it to a RDD of Row objects
    users = lines.map(parseInput)
    datas = lines2.map(datasInput)

    # Convert that to a DataFrame
    usersDataset= spark.createDataFrame(users)
    dataset = spark.createDataFrame(datas)

    # Write it into Cassandra
    usersDataset.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="users", keyspace="movielens") \
        .save()

    dataset.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table = "datas", keyspace = "movielens") \
        .save()

    # Read it back from Cassanda into a new Dataframe
    readUsers= spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="users", keyspace="movielens") \
        .load()

    readData= spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="datas", keyspace="movielens") \
        .load()


    readUsers.createOrReplaceTempView("users")
    readData.createOrReplaceTempView("datas")
```
Next is the code to complete the task:
- (i)
```
    # Calculate the average rating for each movie
    avg_ratings = spark.sql("""
        SELECT movie_id, AVG(rating) AS avg_rating
        FROM datas
        GROUP BY movie_id
        ORDER BY avg_rating DESC
    """)
    avg_ratings.show(10)

```
The results are as follows：

| movie_id| avg_rating|
|-------|-------|
|    1483|       5.0|
|    1127|       5.0|
|     392|       5.0|
|    1265|       5.0|
|    1143|       5.0|
|     580|       5.0|
|    1238|       5.0|
|    1088|       5.0|
|    1618|       5.0|
|    1395|       5.0|

only showing top 10 rows

- (ii)
```
# Identify the top ten movies with the highest average rating
    top_10_movies = spark.sql("""
        SELECT movie_id, AVG(rating) AS avg_rating
        FROM datas
        GROUP BY movie_id
        ORDER BY avg_rating DESC
    """)
    top_10_movies.show(10)
```

The results are as follows：

|movie_id|avg_rating|
|-----|---------|
|    1618|       5.0|
|     580|       5.0|
|    1265|       5.0|
|    1395|       5.0|
|    1127|       5.0|
|    1483|       5.0|
|    1025|       5.0|
|     392|       5.0|
|    1238|       5.0|
|    1088|       5.0|

only showing top 10 rows

- (iii)
```
    activeUsers = spark.sql("""
        SELECT user_id, COUNT(movie_id) AS num_ratings
        FROM datas
        GROUP BY user_id
        HAVING COUNT(movie_id) >= 50
        ORDER BY num_ratings DESC;
    """)
    activeUsers.show(10)
```
Since there may be a problem in reading the u.item data, after many attempts, the problem still cannot be solved, so the process of determining the user's favorite movie genre is not completed.

Finding users who have rated at least 50 movies results in the following:

|user_id|count|
|----|----|
|405	|737|
|655	|685|
|13	 |636|
|450	|540|
|276	|518|
|416	|493|
|537	|490|
|303	|484|
|234	|480|
|393	|448|


- (iv)
```
    sqlDF = spark.sql("SELECT * FROM users WHERE age <20")
    sqlDF.show()
```

The results are as follows：

|user_id|age|gender|occupation|  zip|
|----|------|------|----------|-----|
|    142| 13|     M|     other|48118|
|    482| 18|     F|   student|40256|
|    303| 19|     M|   student|14853|
|    101| 15|     M|   student|05146|
|    872| 19|     F|   student|74078|
|    601| 19|     F|    artist|99687|
|    817| 19|     M|   student|60152|
|    710| 19|     M|   student|92020|
|    246| 19|     M|   student|28734|
|    320| 19|     M|   student|24060|

only showing top 10 rows

- (v)

```
    sql_query_v = spark.sql("""
        SELECT *
        FROM users
        WHERE occupation = 'scientist'
          AND age >= 30
          AND age <= 40
    """)

    sql_query_v.show(10)
```

The results are as follows：


|user_id|age|gender|occupation|zip  |
|----|----|----|----|----|
|272    |33 |M     |scientist |53706|
|430    |38 |M     |scientist |98199|
|538    |31 |M     |scientist |21010|
|183    |33 |M     |scientist |27708|
|643    |39 |M     |scientist |55122|
|543    |33 |M     |scientist |95123|
|71     |39 |M     |scientist |98034|
|730    |31 |F     |scientist |32114|
|74     |39 |M     |scientist |T8H1N|
|107    |39 |M     |scientist |60466|

only showing top 10 rows
