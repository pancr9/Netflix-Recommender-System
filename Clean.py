
# coding: utf-8

# Author: Rekhansh Panchal (rpanchal@uncc.edu) & Aditya Gupta (agupta42@uncc.edu) 
# 
# Cloud Computing for Data Analysis: ITCS 6190 under Dr. Srinivas Akella.
# 
# Program to clean the data as required for input to the recommendation engine.

# In[1]:


'''
Importing Required libraries.
'''
import os, sys


# In[2]:


'''
Define Constants
'''
COMMA = ","
COLON = ":"


# In[3]:


'''
Function to get data in required format.
'''
def cleanDataAndReturnPath(path1, path2):
    f1 = open(path1, 'r')
    f2 = open(path2, 'w')
    count = 1
    for line in f1:
        if(',' in line):
            inp = line.split(",")
            f2.write(str(count) + "," + inp[0] + "," + inp[1] + "\n")
        else:
            inp = line.split(":")
            count = int(inp[0])
        
    f1.close()
    f2.close()
    return path2


# In[ ]:


if __name__ == "__main__":
    
    if len(sys.argv) < 1):
        print >> sys.stderr,             "Usage: Clean <file>"
        exit(-1)  

    path = cleanDataAndReturnPath(sys.argv[0], 'input_data_clean.txt')
    print("Input file stored at:")
    print path

