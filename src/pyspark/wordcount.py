from pyspark import SparkContext


sc = SparkContext("local[*]","wordcount")

print(sc)

file = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/words.txt")

words = file.flatMap(lambda x : x.split(" "))

print(words)

for a in words:
    print(a)


