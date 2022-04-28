import findspark
findspark.init()
from pyspark.sql import SparkSession

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
    # part below is just fir demonstrating that search and filterring for trash words works
    data = rdd.flatMap(lambda x: x.split(" ")).collect() # collected all data in one dataset from the first txt file
    temparr = []
    temparr = data
    for x in temparr[:15]:
        print(x)

    print("\n")
    trash_words = ['was', 'Let', 'is', 'something', 'weird', 'the', 'so', 'happening', 'recently', '\'']
    cleaned = rdd2.flatMap(lambda x: x.split(" ")).filter(lambda x: x not in trash_words).collect()
    for x in cleaned[:15]:
        print(x)
    
 
