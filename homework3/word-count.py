from pyspark import SparkConf, SparkContext
import re

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text)

conf = SparkConf().setMaster("local").setAppName("WordCount")
# conf.SparkContext.setLogLevel("ERROR")
sc = SparkContext(conf = conf)

input = sc.textFile("C://SparkCourse//200911 석간 (보도) 8월의 데이터(D)네트워크(N)인공지능(A) 우수사례 선정.hwp",).map(lambda x: x.x.encode().decode("unicode-escape"))
counts = input.flatMap(lambda line: str(line.encode('utf-8')).split("\n"))\
    .map(lambda line: (line.split("\t")[0], 1))\
    .reduceByKey(lambda a, b: a + b)
# words = input.flatMap(normalizeWords)
# wordCounts = words.countByValue()

for word, count in counts:
    cleanWord = word.encode('utf-8', 'ignore')
    if (cleanWord):
        print(cleanWord.decode('utf-8') + " " + str(count))

spark.stop()