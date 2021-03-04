from lesson6.examples.models.transformation import *
from lesson6.examples.models.loader import *
from spark import initialize_spark
from pyspark.sql.functions import *


def main_recommender():
    """Entry point for the application script"""

    session = initialize_spark()
    movies_path = "/training/data_academy_spark/lesson6/examples/resources/movies.csv"
    ratings_path = "/training/data_academy_spark/lesson6/examples/resources/ratings.csv"

    # load data movies and ratings from csv
    movies = load_from_csv(session, movies_path)
    ratings = load_from_csv(session, ratings_path)

    # sample a user with a set a ratings
    user1 = movies.join(ratings, movies.movieId == ratings.movieId).filter(ratings.userId == 1)
    user1.show(truncate=False)
    print(user1.count())
    print(movies.count())

    # run a the recommender model on the small dataset
    userRecs = recommender(ratings)
    userRecs.printSchema()

    #return a list of top movies expected for the same user
    userRec1 = userRecs.filter(userRecs.userId == 1) \
        .withColumn('ratings', explode(userRecs.recommendations)) \
        .select('userId', 'ratings.movieId', 'ratings.rating')

    userRec1.join(movies, movies.movieId == userRec1.movieId) \
        .show(truncate=False)


def main_prediction():
    session = initialize_spark()
    data_path = "/training/data_academy_spark/lesson6/examples/resources/data.csv"
    df = load_from_csv(session, data_path)

    # Basics stats from our columns
    sample = df.describe().toPandas()
    print(sample)

    #Data preparation and feature engineering
    data = prepare(df)
    indexed_data = indexer(data)
    vector_data = add_vector(indexed_data)
    predicted = prediction(vector_data)
    predicted.show()
    evaluate(predicted)

if __name__ == '__main__':
    main_prediction()
