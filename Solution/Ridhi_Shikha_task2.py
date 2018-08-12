
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

ratings1 = pd.read_csv("C:\\Users\\ridhi\\OneDrive\\Documents\\inf 553\\small\\ratings.csv")
tags1 = pd.read_csv("C:\\Users\\ridhi\\OneDrive\\Documents\\inf 553\\small\\tags.csv")
rating = pd.DataFrame(ratings1)
tag = pd.DataFrame(tags1)

#remove whitespace
rating.columns = rating.columns.str.replace('\s+', '')       
tag.columns = tag.columns.str.replace('\s+', '') 

#choose specific columns
cols1= rating[["movieId","rating"]]                
cols2 = tag[["movieId","tag"]]

#Join the two sets

Join_result = pd.merge(cols1,cols2,left_index=True, right_index=True, how='inner',on='movieId')

Tags123= Join_result[["tag","rating"]]

#group by tag and mean of rating
avg= Tags123.groupby(["tag"])["rating"].mean()

#sort
sorted_by_tag = avg.sort_index(ascending=False,na_position='last')

#Join_result=cols1.join(cols2, on='movieId',how='inner',lsuffix='_left', rsuffix='_right') 
#avg= Join_result.groupby(["tag"])["rating"].mean()  

#print (sorted_by_tag)

#write to file
File_Write = open('C:\\Ridhi_Shikha_HW1\\Ridhi_Shikha\\OutputFiles\\Ridhi_Shikha_result_task2_small.csv', 'w')
#add header
header = ['tag','ratings']  
csvEdit = csv.writer(File_Write)
csvEdit.writerow(header)

#convert dataframe to CSV
sorted_by_tag.to_csv(File_Write)  
File_Write.close()




