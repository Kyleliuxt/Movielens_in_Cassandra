from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import avg, col, count
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

def parseInputUser(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])

def parseInputRating(line):
    fields = line.split('\t')
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), timestamp=int(fields[3]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieLensCassandraIntegration") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .getOrCreate()

    userLines = spark.sparkContext.textFile("hdfs:///user/maria_dev/lxt/u.user")
    users = userLines.map(parseInputUser)
    usersDataset = spark.createDataFrame(users)

    ratingLines = spark.sparkContext.textFile("hdfs:///user/maria_dev/lxt/u.data")
    ratings = ratingLines.map(parseInputRating)
    ratingsDataset = spark.createDataFrame(ratings)

    keyspace = 'movielens'
    table = 'users'

    usersDataset.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="users", keyspace="movielens")\
        .save()

    ratingsDataset.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="ratings", keyspace="movielens")\
        .save()


    readUsers = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="users", keyspace="movielens")\
    .load()

    readRatings = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="ratings", keyspace="movielens")\
    .load()


    readUsers.createOrReplaceTempView("users")
    readRatings.createOrReplaceTempView("ratings")

    # Users under 20 years old
    spark.sql("SELECT * FROM users WHERE age < 20 LIMIT 10").show()

    # Average rating for each movie
    spark.sql("SELECT movie_id, AVG(rating) as average_rating FROM ratings GROUP BY movie_id LIMIT 10").createOrReplaceTempView("movie_averages")

    # Top 10 movies with the highest average ratings
    spark.sql("SELECT * FROM movie_averages ORDER BY average_rating DESC LIMIT 10").show()

    # Users who have rated at least 50 movies
    spark.sql("""
        SELECT user_id, COUNT(movie_id) as total_movies
        FROM ratings
        GROUP BY user_id
        HAVING total_movies >= 50
        LIMIT 10
    """).show()

    # Scientists aged between 30 and 40
    spark.sql("SELECT * FROM users WHERE occupation = 'scientist' AND age BETWEEN 30 AND 40 LIMIT 10").show()

    spark.stop()
