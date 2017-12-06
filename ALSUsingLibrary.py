
# coding: utf-8

# **Cloud Computing for Data Analytics - ITCS 6190** <br>
# **Recommender System - Using ALS Algorithm Library** <br>
# **Author:** Aditya Gupta, Rekhansh Panchal <br>
# **email:** agupta42@uncc.edu, rpanchal@uncc.edu <br>

# In[1]:


'''
Importing required libraries.
'''
import os, sys
import numpy as np
from pyspark import SparkContext
from pyspark.mllib.recommendation import MatrixFactorizationModel, Rating
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row, SparkSession
from pyspark.ml.recommendation import ALS


# In[2]:


def getRatings(x):
    
    print x.collect()


# In[3]:


def getRecommendationsFromALSLibrary():
    
    '''
    Build the recommendation model using Alternating Least Squares
    '''
    
    parts = ratings_input.map(lambda row: row.value.split(","))
    ratingsRDD = parts.map(lambda p: Row(userId=int(p[1]), movieId=int(p[0]), rating=float(p[2])))
    ratings = spark.createDataFrame(ratingsRDD)
    

    #Need a training and test set into 85 and 15%
    train, test = ratings.randomSplit([0.85,0.15],123)

    print ("Training Count for Ratings", train.count())
    print ("Test Count for Ratings",test.count()) 
    
    # Cache Training and Test Data
    train.cache()
    test.cache()
    
    # Generate ALS Model.
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
    model = als.fit(train)

    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Square Error = " + str(rmse))

    # Generate Top 10 movie recommendations for each user
    userRecs = model.recommendForAllUsers(10)                              
    
    print ("Printing Recommendations for All Users: ")
    
    print (userRecs.collect())


# In[4]:


if __name__ == "__main__":
    
    if len(sys.argv) < 1):
        print >> sys.stderr,             "Usage: ALSUsingLibrary <file>"
        exit(-1)   

    spark = SparkSession            .builder            .appName("ALSUsingLibrary")            .getOrCreate()

    ratings_input = spark.read.text(sys.argv[0]).rdd

    getRecommendationsFromALSLibrary()

    spark.stop()

