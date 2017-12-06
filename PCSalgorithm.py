# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <markdowncell>

# **Cloud Computing for Data Analytics - ITCS 6190** <br>
# **Recommender System** <br>
# **Author:** Aditya Gupta, Rekhansh Panchal <br>
# **email:** agupta42@uncc.edu, rpanchal@uncc.edu <br>

# <codecell>

# Importing Required libraries.

import os, sys
import pandas as pd
import numpy as np
import math
from pyspark import SparkContext

# <codecell>

# make an array with unique users.

def users(utility):
    user_id = []
    for a in utility:
        user_id.append(a)
        
    return set(user_id)

# <codecell>

# Pearson Coefficient Similarity Calculation.

def pearson(utility, u1, u2):
    movies = {}
    for movie in utility[u1]:
        if movie in utility[u2]:
            movies[movie] = 1
    length = len(movies)
    if length == 0:
        return 0
    
    sum_x = sum_xy = sum_xx = sum_y = sum_yy = 0
        
    for movie in movies:
        sum_x += utility[u1][movie]
        sum_y += utility[u2][movie]
        
        sum_xx += pow(utility[u1][movie], 2)
        sum_yy += pow(utility[u2][movie], 2)
        sum_xy += (utility[u1][movie]*utility[u2][movie])
        
    # Implementing the Pearson's Coefficient formula.
    numerator = length*sum_xy - (sum_x*sum_y)
    
    denominator_square = (length*sum_xx-pow(sum_x, 2))*(length*sum_yy-pow(sum_y, 2))
    denominator = pow(denominator_square, 0.5)
    
    if denominator == 0:
        return 0
    else:
        return numerator/denominator

# <codecell>

def getRecommendations(user, mov):
    totals = {}
    similarity_sums = {}
    rankings = []
    for other in users(utility):
        if other == user:
            continue
        similarity = pearson(utility, user, other)
        
        if similarity <= 0:
            continue
        # print("Similarity between " + str(user) + " and " + str(other) + " is " + str(similarity))
        for movie in utility[other]:
        
            if movie not in utility[user] or utility[user][movie] == 0:
                
                totals.setdefault(movie, 0)
                totals[movie] += utility[other][movie]*similarity
                similarity_sums.setdefault(movie, 0)
                similarity_sums[movie] += similarity
                
    rankings = [(total/similarity_sums[movie],movie) for movie, total in totals.items()]
    rankings.sort()
    rankings.reverse()
    topRankings = rankings[0:50]
    recommendationNum = [movie for score, movie in topRankings]
    recommendationList = []
    
    for a in recommendationNum:
        recommendationList.append((mov.take(mov.count())[a-1][1]).encode('ascii', 'ignore'))
    return recommendationList

# <codecell>

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print >> sys.stderr, \
            "Usage: PCSalgorithm <file>"
        exit(-1)    
        
    # /home/aditya/Downloads/data
    sc = SparkContext.getOrCreate()
    ratings_input = sc.textFile(sys.argv[1])
    
    split_data = ratings_input.map(lambda x:x.split(','))
    movie = split_data.map(lambda y: int(y[0]))
    user = split_data.map(lambda y: int(y[1]))
    rating = split_data.map(lambda y: int(y[2]))
    
    mapped_ratings = ratings_input.map(lambda l: l.split(','))
    
    # ratings list will have [movieId, userId, rating]
    ratings_list = mapped_ratings.map(lambda x: (int(x[0]),int(x[1]), float(x[2])))
    
    # read the movies file
    movieFile = sc.textFile(sys.argv[2])
    movieSplit = movieFile.map(lambda x:x.split(','))
    movie = movieSplit.map(lambda y: (y[0], y[2]))
    
    # generate user-movie-rating matrix. 
    utility = {}
    for a in range(rating.count()):
        userId = user.take(user.count())[a]
        movieName = ratings_list.take(movie.count())[a][0]
    
        rate = rating.take(rating.count())[a]
        utility.setdefault(userId, {})
        utility[userId][movieName]= rate
       
    
    #User id of current user
    
    user_rec = int(sys.argv[3])
    inputId = user.filter(lambda a: a == user_rec).count()
    if inputId == 0:
        print "Please enter the correct user Id."
    else:
        getrec = getRecommendations(user_rec, movie)
        if not getrec:
            print "No movies to recommend"
        else:
            print("Your recommended movies are" + '\n' + str(getrec))
    
    sc.stop()

