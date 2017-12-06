
# coding: utf-8

# In[1]:


'''
Importing required libraries.
'''
import os, sys
import numpy as np
from pyspark import SparkContext


# In[2]:


# Function to check similarity
def getRecommendations(user, mov):
    totals = {}
    similarity_sums = {}
    rankings = []
    for other in users(utility):
        if other == user:
            continue
        similarity = calcALS(utility, user, other)
    
        if similarity <= 0:
            continue
        
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


# In[3]:


# Function to return list of unique users.

def users(utility):
    user_id = []
    for a in utility:
        user_id.append(a)
        
    return set(user_id)


# In[4]:


# Function to calculate cosine similarity between two users
def calcALS(utility, u1, u2):
    
    movies = {}
    
    for movie in utility[u1]:
        if movie in utility[u2]:
            movies[movie] = 1
    length = len(movies)
        
    if length == 0:
        return 0
    
    sum_xy = sum_xx = sum_yy = 0
    
    for movie in movies:
        sum_xx += pow(utility[u1][movie], 2)
        sum_yy += pow(utility[u2][movie], 2)
        sum_xy += (utility[u1][movie]*utility[u2][movie])
    
    numerator = sum_xy
    denominator = pow(sum_xx * sum_yy , 0.5)
    
    if denominator == 0:
        return 0
    else:
        return numerator / denominator


# In[5]:


if __name__ == "__main__":
    
    if len(sys.argv) < 3):
        print >> sys.stderr,             "Usage: ALS <file>"
        exit(-1)    

    sc = SparkContext.getOrCreate()
    ratings_input = sc.textFile(sys.argv[1])

    split_data = ratings_input.map(lambda x:x.split(','))
    movie = split_data.map(lambda y: int(y[0]))
    user = split_data.map(lambda y: int(y[1]))
    rating = split_data.map(lambda y: int(y[2]))

    unique_users = user.distinct()
    mapped_ratings = ratings_input.map(lambda l: l.split(','))

    # ratings list will have [movieId, userId, rating]
    ratings_list = mapped_ratings.map(lambda x: (int(x[0]),int(x[1]), float(x[2])))
    print ("Some Ratings List", ratings_list.take(3))

    # read the movies file
    movieFile = sc.textFile(sys.argv[2])
    movieSplit = movieFile.map(lambda x:x.split(','))
    movie = movieSplit.map(lambda y: (int(y[0]), y[2]))
    print ("Some Movies are", movie.take(3))

    # generate user-movie-rating matrix. 

    utility = {}
    for a in range(rating.count()):
        userId = user.take(user.count())[a]
        movieName = ratings_list.take(movie.count())[a][0]

        rate = rating.take(rating.count())[a]
        utility.setdefault(userId, {})
        utility[userId][movieName]= rate

    user_rec = int(sys.argv[3]);
    print("Your recommended movies are" + '\n' + str(getRecommendations(user_rec, movie)))
    sc.stop()

