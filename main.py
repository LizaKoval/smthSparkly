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
    rdd_for_txt_file = text_bin.flatMap(lambda x: x.split(" "))\
        .persist(pyspark.StorageLevel.MEMORY_AND_DISK)\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[1], x[0]))\
        .sortByKey(ascending=False)\
        .take(10)

# найти способ как отобразить эти 10 слов эффективнее
    for x in rdd_for_txt_file:
        print(x)
# я собираю все данные, только чтобы напечатать 15 первых слов, а надо найти  способ вывести первые 15 слов из rdd
# -------------------------------------------removing trash words from the text-----------------------------------------------------------------------------
    trashwords_bin = spark.sparkContext.textFile("input_data/trash_words.txt")
    unique_trashwords = trashwords_bin.flatMap(lambda x: x.split(" "))\
         .distinct()\
         .collect()

    trashwords_bs = spark.sparkContext.broadcast(unique_trashwords) # creating shared broadcast variable

    resulting_text = rdd_for_txt_file.filter(lambda word: word not in trashwords_bs).take(15)

    for x in resulting_text[:15]:
      print(x)

    # part below is just for demonstrating that search and filterring for trash words works
    # don't use it г have persist already words_from_txt = text_bin.flatMap(lambda x: x.split(" ")).collect() # collected all data in one dataset from the first txt file
    # temparr = []
    # temparr = words_from_txt
    # for x in resulting_text[:15]:
    #     print(x)
    #
    # # removing unnecessary words
    # print("\n")
    # cleaned_txt = trashwords_bin.flatMap(lambda x: x.split(" ")).collect()
    # for x in cleaned_txt[:15]:
    #     print(x)

    timer.stop()
    # cleaned_txt = trashwords_bin.flatMap(lambda x: x.split(" ")).filter(lambda x: x not in trash_words) #  а было collect save as txt()
    # for x in cleaned[:15]:
    #     print(x)
    
 
