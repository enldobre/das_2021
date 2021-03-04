from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def recommender(dataset):
    (training, test) = dataset.randomSplit([0.8, 0.2])
    als = ALS(maxIter=5, regParam=0.01, rank=5, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop")
    model = als.fit(training)

    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

    # Generate top 10 movie recommendations for each user
    userRecs = model.recommendForAllUsers(10)
    return userRecs


def prediction(dataset):
    (training, test) = dataset.randomSplit([0.8, 0.2])
    rf = RandomForestClassifier(labelCol='Survived',
                                featuresCol='features',
                                maxDepth=5)
    model = rf.fit(training)
    predictions = model.transform(test)
    return predictions


def prepare(df):
    dataset = df.select(col('Survived').cast('float'),
                        col('Pclass').cast('float'),
                        col('Sex'),
                        col('Age').cast('float'),
                        col('Fare').cast('float'),
                        col('Embarked')
                        )
    ds = dataset.replace('?', None) \
        .dropna(how='any')
    return ds


def indexer(dataset):
    dataset = StringIndexer(
        inputCol='Sex',
        outputCol='Gender',
        handleInvalid='keep').fit(dataset).transform(dataset)
    dataset = StringIndexer(
        inputCol='Embarked',
        outputCol='Boarded',
        handleInvalid='keep').fit(dataset).transform(dataset)
    dataset.drop('Sex', 'Embarked')
    return dataset


def add_vector(dataset):
    required_features = ['Pclass',
                         'Age',
                         'Fare',
                         'Gender',
                         'Boarded'
                         ]
    assembler = VectorAssembler(inputCols=required_features, outputCol='features')
    transformed_data = assembler.transform(dataset)
    return transformed_data


def evaluate(dataset):
    evaluator = MulticlassClassificationEvaluator(
        labelCol='Survived',
        predictionCol='prediction',
        metricName='accuracy')
    accuracy = evaluator.evaluate(dataset)
    print('Test Accuracy = ', accuracy)
