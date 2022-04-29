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
    #persist_text_words = text_bin.flatMap(lambda x: x.split(" ")).persist(pyspark.StorageLevel.MEMORY_AND_DISK).collect()\

    rdd_for_txt_file = text_bin.flatMap(lambda x: x.split(" "))\
        .map(lambda x: (x,1))\
        .reduceByKey(lambda x, y: x+y)\
        .sortBy(lambda x: x[1], ascending=False)\
        .persist(pyspark.StorageLevel.MEMORY_AND_DISK)

    top_ten = rdd_for_txt_file.take(10)

# поменять reduce на что-то получше
    for x in top_ten:
        print(x)

    print('\n')
# -------------------------------------------removing trash words from the text-----------------------------------------------------------------------------
    trashwords_bin = spark.sparkContext.textFile("input_data/trash_words.txt")
    unique_trashwords = set(trashwords_bin.flatMap(lambda x: x.split(" ")).collect())
    trashwords_bs = spark.sparkContext.broadcast(unique_trashwords) # creating shared broadcast variable

    resulting_text = text_bin.flatMap(lambda x: x.split(" "))\
         .filter(lambda word: word not in trashwords_bs.value)\
         .take(10)

    for x in resulting_text:
        print(x)

    timer.stop()

    
 
