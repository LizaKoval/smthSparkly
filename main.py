import findspark
import pyspark
findspark.init()
from timer import Timer
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
        .appName("NewSparkTry") \
        .getOrCreate()

    timer = Timer()
    timer.start()
# -------------------------------------------counting top 10 words in the "Let my people go" song-----------------------------------------------------------
    text_bin = spark.sparkContext.textFile("input_data/input.txt")
#      # .persist(pyspark.StorageLevel.MEMORY_AND_DISK)\
    rdd_for_txt_file = text_bin.flatMap(lambda x: x.split(" "))\
        .map(lambda x: (x, 1))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[1], x[0]))\
        .sortByKey(ascending=False)\
        .take(10)
# поменять reduce на что-то получше
    for x in rdd_for_txt_file:
        print(x)

    print('\n')
# найти способ как отобразить эти 10 слов эффективнее
#     for x in rdd_for_txt_file:
#         print(x)
# я собираю все данные, только чтобы напечатать 15 первых слов, а надо найти  способ вывести первые 15 слов из rdd
# -------------------------------------------removing trash words from the text-----------------------------------------------------------------------------
    trashwords_bin = spark.sparkContext.textFile("input_data/trash_words.txt")
    unique_trashwords = set(trashwords_bin.flatMap(lambda x: x.split(" ")).collect())
    #trashwords_bs = spark.sparkContext.broadcast(unique_trashwords) # creating shared broadcast variable

    resulting_text = text_bin.flatMap(lambda x: x.split(" "))\
         .filter(lambda word: word not in unique_trashwords)\
         .take(10)

    for x in resulting_text:
        print(x)

    timer.stop()

    
 
