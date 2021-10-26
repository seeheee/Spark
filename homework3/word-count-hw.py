import re, sys
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text)

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("C://SparkCourse//200911석간(보도)8월의데이터(D)네트워크(N)인공지능(A)우수사례선정.txt")

words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

Top_ten = wordCountsSorted.top(11)

for result in Top_ten:
    count = str(result[0])
    word = result[1].encode('utf-8', 'ignore')
    if (word):
        print(word.decode('utf-8') + "\t\t" + count)

