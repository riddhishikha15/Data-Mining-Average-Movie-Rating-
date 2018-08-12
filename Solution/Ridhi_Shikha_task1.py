
# coding: utf-8

# In[1]:


import findspark
findspark.init()
import csv

from pyspark.sql import SQLContext 
from pyspark import SparkContext 


# In[2]:


import pandas as pd
sc= SparkContext('local','task1')
sqlContext = SQLContext(sc)

ratings = pd.read_csv("C:\\Users\\ridhi\\OneDrive\\Documents\\inf 553\\small\\ratings.csv")
df2= pd.DataFrame(ratings)
df2.columns = df2.columns.str.replace('\s+', '')  #remove whitespace
cols= df2[["movieId","rating"]]                    #choose columns
avg= cols.groupby("movieId")["rating"].mean()         #group by movieId and mean of rating

#write to file
File_Write = open('C:\\Ridhi_Shikha_HW1\\Ridhi_Shikha\\OutputFiles\\Ridhi_Shikha_result_task1_small.csv', 'w')
header = ['movieId','ratings']   #add header
csvEdit = csv.writer(File_Write)
csvEdit.writerow(header)
avg.to_csv(File_Write)   #convert dataframe to CSV
File_Write.close()




