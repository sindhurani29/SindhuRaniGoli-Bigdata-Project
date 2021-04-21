# SindhuRaniGoli-Bigdata-Project
## Author: 
Sindhu Rani Goli
## Text Data Source: 
- I have taken the text data from the website:
 https://www.gutenberg.org/
- I have taken the text data from the URL:
 https://www.gutenberg.org/files/65122/65122-0.txt
## Tools and Languages used:
### Language: 
- Python
### Tools: 
- Pyspark
- Databricks Notebook 
- Pandas
- MatPlotLib 
- Regex
- Urllib
## Process:
1. To start, we'll request or pull data from the text data's url using the urllib.request library. After the data is pulled, it is saved in a temporary file named 'sindhugoli.txt,' which will get the text data from https://www.gutenberg.org/ website, 'You'll Like It On Mars'
```
# Obtain the text data from URL
import urllib.request
stringInURL = "https://www.gutenberg.org/files/65122/65122-0.txt"
urllib.request.urlretrieve(stringInURL, "/tmp/sindhurani.txt")
```
2. We'll use dbutils.fs.mv to move the data from the temporary data to a new site called data now that it's been saved.
```
dbutils.fs.mv("file:/tmp/sindhurani.txt", "dbfs:/data/sindhurani.txt")
```
3. In this step, we will use sc.textfile to import the data file into Spark's RDD (Resilient Distributed Systems), which is a collection of elements that can be worked on in parallel. RDDs can be created in two ways: by parallelizing an existing array in your driver software, or by referencing a dataset in an external storage system like a shared filesystem, HDFS, HBase, or some other data source that supports RDDs.
```
sindhurani_RDD = sc.textFile("dbfs:/data/sindhurani.txt")
```
### Data cleaning
4. The capitalized phrases, sentences, punctuations, and stopwords are all included in the above data file (words that do not add much meaning to a sentence Examples: the, have, etc.). We will break down the data using flatMap in the first step of cleaning it, changing any capitalized terms to lower case, eliminating any spaces, and breaking sentences into words.
```
# flatmap each line to words
wordsRDD = sindhurani_RDD.flatMap(lambda line : line.lower().strip().split(" "))
```
5. The punctuation comes next. For anything that does not resemble a text, we will import the re(regular expression) library.
```
import re
CleanTokensRDD = wordsRDD.map(lambda w: re.sub(r'[^a-zA-Z]','',w))
```
6. The next step is to remove any stopwords with pyspark.ml.feature by importing stopwords remover.
```
#prepare to clean stopwords
from pyspark.ml.feature import StopWordsRemover
remove =StopWordsRemover()
stopwords = remove.getStopWords()
CleanWordsRDD=CleanTokensRDD.filter(lambda wrds: wrds not in stopwords)
```
### Data Processing:
7. After cleaning the data, the next step is to process it. Our words will be converted into intermediate key-value pairs. For each word variable in the RDD, we'll make a pair of (", 1) pairs. We can consider a pair RDD by combining the map() transformation with the lambda() function. Once we map it, it will look like this: (word,1).
```
IKVPairsRDD= CleanWordsRDD.map(lambda word: (word,1))
```
8. We'll perform the Reduce by key operation in this step. The word is the answer. We'll keep track of the first word count each time. If the word appears again, the most recent one will be removed and the first word count will be kept.
```
Word_Count_RDD = IKVPairsRDD.reduceByKey(lambda acc, value: acc+value)
```
9. We will retrieve the elements in this step. The collect() action function is used to return to the driver program all elements from the dataset(RDD/DataFrame/Dataset) as an Array(row).
 ``` 
finalresults = Word_Count_RDD.collect()
 ```
10. Sorting a list of tuples by the second value, which will be used to reorder the list's tuples. Tuples' second values are mentioned in ascending order. The top 10 words are shown in print below.
 ```
finalresults = Word_Count_RDD.map(lambda x: (x[1], x[0])).sortByKey(False).take(10)
print(finalresults)
```
11. The library mathplotlib will be used to graph the results. By plotting the x and y axis, we can show any form of graph (Ex: bar, scatter, pie).
```
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
```
## Charting Results
![sorted results](https://github.com/sindhurani29/sindhuranigoli-bigdata-finalproject/blob/main/final1.PNG)
![charted results](https://github.com/sindhurani29/sindhuranigoli-bigdata-finalproject/blob/main/final2.PNG)
## References:
- [PySpark](https://github.com/denisecase/starting-spark)
- [Databricks](https://docs.databricks.com/)
- [Dbutils](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils)
- [Gutenberg](https://www.gutenberg.org/)




