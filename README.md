## Cloud Computing for Data Analysis: Movie Recommendation System

<img src="https://github.com/pancr9/Netflix-Recommender-System/blob/master/205.jpg" alt="Drawing" style="width: 200px;"/>

#### Authors: 
Aditya Gupta (agupta42@uncc.edu) & Rekhansh Panchal (rpanchal@uncc.edu)

Group 11


***

#### Overview:

* The project aims to analyze and extract insights from the Netflix data using the concepts of Cloud Computing.
* The goal of the project is to implement Pearson Correlation Coefficient & Alternating Least Squares algorithms with the help of PySpark.
* Movie Recommendations is implemented using Collaborative Filtering using pySpark on Netflix Data.
* This project’s primary aim is to provide movie recommendations to the user based on their
preferences.

***
#### What is Collaborative Filtering:
Collaborative Filtering is a method of making automatic predictions (filtering) about the interests of a user by collecting preferences or taste information from similar users.

***

#### Data:
The original movie rating files contain over 100 million ratings from 480 thousand randomly-chosen, anonymous Netflix customers over 17 thousand movie titles. 

The data were collected between October, 1998 and December, 2005 and reflect the distribution of all ratings received during this period. The ratings are on a scale from 1 to 5 (integral) stars. 

However, use have worked on a part of the complete data for the project.

***

#### Data Link:
[Movie Rating Files](https://www.kaggle.com/netflix-inc/netflix-prize-data/data)

***

#### Technique:
1. Pearson Correlation Coefficient

2. Alternative Least Squares

***

#### Expectations
##### 1. What to expect?

* One ​can ​expect ​the ​implementation ​of ​both ​the ​algorithms ​and ​a ​proper documentation ​of ​outcomes ​of ​this ​project, which ​is ​the ​movie ​recommendations for ​users.

##### 2. Likely to accomplish

* Result comparison and Performance Evaluation with respect to existing implementation of algorithms and project implementation.
* Documentation and online-publishing of the codebase.

##### 3. Ideal Accomplishments.

* Suggested modifications/changes in the existing or project implementation.
* Creating a easy to use library that one can use for analysis purpose.

***

#### Tools Used:
* Jupyter Notebook
* pySpark
* Git and GitHub
* Amazon S3 and EC2

***

#### README


* Add all program files to the Amazon Storage S3 along with the reduced dataset of ratings.
* Create a Spark Cluster on Amazon EC2 and get the details of the cluster to use it on Terminal.

* Confirm Connection to the Cluster with obtained keypair.
~~~~
ssh -i ~/keypair.pem -ND 8157 hadoop@ec2-34-238-246-242.compute-1.amazonaws.com
~~~~

* Start Cluster Access
~~~~
ssh -i keypair.pem hadoop@ec2-34-238-246-242.compute-1.amazonaws.com
~~~~

* Import Pandas on the cluster:
~~~~
sudo pip install pandas
~~~~

* Run PCSalgorithm on input data consisting 4,20,000 ratings stored on S3 Storage. To recommend movies for user 1199825.
~~~~
spark-submit s3://itcs6190/PCSalgorithm.py s3://itcs6190/movie_input_ratings.txt s3://itcs6190/movie_titles.csv 1199825
~~~~

* Run ALS on input data consisting 4,20,000 ratings stored on S3 Storage. To recommend movies for user 1199825.
~~~~
spark-submit s3://itcs6190/ALS.py s3://itcs6190/movie_input_ratings.txt s3://itcs6190/movie_titles.csv 1199825
~~~~

* Run ALS using library for recommend movies for all users
~~~~
spark-submit s3://itcs6190/ALSUsingLibrary.py s3://itcs6190/movie_input_ratings.txt
~~~~

#### Results:

The programs to recommend were ran on Amazon EC2 Spark cluster. And satisfactory recommendations were obtained using 3 methods.
* Using Pearson Correlation ( User - User based )
* Using ALS implementation 
* Using ALS library from mllib ( User - User and Item - Item based )


***

#### Conclusion:

Created a User - User based recommendation system using ALS and Pearson Correlation Coefficient techniques.
Displayed top movies recommended a user by taking userId as input.

***

#### Future Scope:

***

#### Challenges Faced:

* We started with implementing Singular Value Decomposition technique, but couldn't achieve anything potential with that.
* Had no prior experience on implementing the code on PySpark, so had a lot of minor issues while handling the data.
* The data available is huge for to be considered, hence we had to limit it down to a lower scale.

***
