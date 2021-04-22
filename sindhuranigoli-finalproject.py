# Databricks notebook source
# get text data from url
import urllib.request
stringInURL = "https://www.gutenberg.org/files/65122/65122-0.txt"
urllib.request.urlretrieve(stringInURL, "/tmp/sindhurani.txt")


# COMMAND ----------

dbutils.fs.mv("file:/tmp/sindhurani.txt", "dbfs:/data/sindhurani.txt")

# COMMAND ----------

sindhurani_RDD = sc.textFile("dbfs:/data/sindhurani.txt")

# COMMAND ----------

# flatmap each line to words
wordsRDD = sindhurani_RDD.flatMap(lambda line : line.lower().strip().split(" "))

# COMMAND ----------

import re
CleanTokensRDD = wordsRDD.map(lambda w: re.sub(r'[^a-zA-Z]','',w))

# COMMAND ----------

#prepare to clean stopwords
from pyspark.ml.feature import StopWordsRemover
remove =StopWordsRemover()
stopwords = remove.getStopWords()
CleanWordsRDD=CleanTokensRDD.filter(lambda wrds: wrds not in stopwords)

# COMMAND ----------

IKVPairsRDD= CleanWordsRDD.map(lambda word: (word,1))

# COMMAND ----------

Word_Count_RDD = IKVPairsRDD.reduceByKey(lambda acc, value: acc+value)

# COMMAND ----------

finalresults = Word_Count_RDD.collect()

# COMMAND ----------

finalresults = Word_Count_RDD.map(lambda x: (x[1], x[0])).sortByKey(False).take(10)
print(finalresults)

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

source = 'You ll Like It On Mars'
title = 'Top Words in ' + source
xlabel = 'Number of times used'
ylabel = 'Words'

# create Pandas dataframe from list of tuples
df = pd.DataFrame.from_records(finalresults, columns =[xlabel, ylabel]) 
print(df)

# create plot (using matplotlib)
plt.figure(figsize=(8,4))
sns.barplot(xlabel, ylabel, data=df, palette="Blues_d").set_title(title)



# COMMAND ----------


