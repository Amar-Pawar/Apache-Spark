'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-08-16
@Last Modified by: Amar Pawar
@Last Modified time: 2021-08-16
@Title : Word count program with PySpark
/**********************************************************************************
'''
from pyspark import SparkContext
from logging_handler import logger

def word_count():
    """
    Description:
        This function will count words that are present in file provided in path and willstore output.
    """
    try:
        sc = SparkContext("local","PySpark word count program")
        words = sc.textFile("hdfs://localhost:9000/Sample/sample_data.txt").flatMap(lambda x:x.split(" "))
        wordcounts = words.map(lambda word: (word,1)).reduceByKey(lambda a,b:a+b)
        wordcounts.saveAsTextFile("hdfs://localhost:9000/Sample/word_count")
        logger.info("Output stored Successfully")
    except Exception as e:
        logger.info(f"Erorr!!{e}")

word_count()
