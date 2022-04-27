import findspark
from nltk.corpus import stopwords
findspark.init()
from pyspark.sql import SparkSession

# from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
        .appName("NewSparkTry") \
        .getOrCreate()

    rdd = spark.sparkContext.textFile("input_data/input.txt")
    rdd3 = rdd.flatMap(lambda x: x.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)

    rating = rdd3.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).take(10)
    for x in rating:
        print(x)
# -------------------------------------------second task-----------------------------------------------------------
    rdd2 = spark.sparkContext.textFile("input_data/text_with_trash.csv")

    data = rdd.flatMap(lambda x: x.split(" ")).collect() # collected all data in one dataset from the first txt file
    temparr = []
    temparr = data
    for x in temparr[:15]:
        print(x)
    print("\n")
    cleaned = rdd2.flatMap(lambda x: x.split(" ")).filter(lambda x: x in temparr).collect()
    for x in cleaned[:15]:
        print(x)



    print("\n")
    # cleaned = output.filter(lambda word: ord not in stopwords)
    #
    # for x in output:
    #      print(x)



    # cleaned = output.filter(lambda word:  ord not in stopwords)


#    print(rdd2_words.collect())
    # pattern_punct = '[:\;№\$\!-\~was#+Let]'
#
#
#     bin = spark.sparkContext.textFile("input_data/text_with_trash.txt") #created RDD with data from text file


# data = [("L", 1), ("I", 20), ("Z", 30), ("A", 40), ("B", 30), ("A", 60)]
# inputRDD = spark.sparkContext.parallelize(data)
#
# listRdd = spark.sparkContext.parallelize([5, 2, 3, 4, 1, 3, 2])
#
# print("fold: " + str(listRdd.fold(0,add)))
# print("first: " +str(inputRDD.first()))
# print("top: "+str(listRdd.top(2)))
# print("top: " + str(inputRDD.top(2)))
#
# # take, takeOrdered, takeSample
# print("take : " + str(listRdd.take(2)))
# # Output: take : 1,2
# print("takeOrdered : " + str(listRdd.takeOrdered(2)))
# # Output: takeOrdered : 1,2
# print("take : " + str(listRdd.takeSample(0,2)))
