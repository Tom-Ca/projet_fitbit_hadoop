import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
import statistics
import pyspark.sql.functions as F
from pyspark.sql.functions import lit

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from tqdm import tqdm
spark = SparkSession.builder.getOrCreate()
sparkContext = spark.sparkContext
# sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()


def tab1():
    columns = StructType([StructField('TS',
                                      StringType(), True),
                          StructField('BPM',
                                      StringType(), True),
                          StructField('Date',
                                      StringType(), True)])

    # Create a dataframe with expected schema
    df_full = spark.createDataFrame(data=[], schema=columns)
    list_file = [f for f in os.listdir('./Ressources/activities_heart')]
    print(list_file)
    for file in tqdm(list_file):
        df = spark.read.option("multiline", "true").json(f"Ressources/activities_heart/{file}")
        # df = spark.read.option("multiline", "true").json(f"Ressources/activities_heart/*.json")
        try:
            data_collect = df.collect()
            data_collect = data_collect[0]["activities-heart-intraday"]["dataset"]
            # print(data_collect,file)
            if data_collect != []:
                df2 = spark.createDataFrame(data_collect, schema='time string, value string')
                df2 = df2.withColumn("date", lit(file[:-5]))
                # df_full = df_full.union(df2)
                df2.write.csv(f'./Ressources/activities_heart_csv/{file[:-5]}.csv')

        except:
            pass
    # df_full.write.csv('tab1.csv')

def stat_per_day():

    columns2 = StructType([StructField('TimeStamp',
                                       StringType(), True),
                           StructField('Minimum',
                                       StringType(), True),
                           StructField('Maximum',
                                       StringType(), True),
                           StructField('Moyenne',
                                       StringType(), True),
                           StructField('Mediane',
                                       StringType(), True)])

    df_full = spark.createDataFrame(data=[], schema=columns2)
    list_file = [f for f in os.listdir('./Ressources/activities_heart_csv')]
    # print(list_file)
    for file in tqdm(list_file):
        columns1 = StructType([StructField('TS',
                                          StringType(), True),
                              StructField('BPM',
                                          StringType(), True),
                              StructField('Date',
                                          StringType(), True)])


        df = spark.read.csv(f'./Ressources/activities_heart_csv/{file}', schema=columns1)
        l1 = df.select('BPM').rdd.flatMap(lambda x: x).collect()
        integer_map = map(int, l1)
        l1 = list(integer_map)
        # print(statistics.median(l1), statistics.mean(l1), min(l1),max(l1))
        newRow = spark.createDataFrame([(file[:-4], min(l1), max(l1),  statistics.mean(l1), statistics.median(l1))], columns2)
        df_full = df_full.union(newRow)
        # df_full.show()
    df_full.write.csv(f'./Ressources/ALL_stat.csv')



def stat_per_H():
    columns2 = StructType([StructField('HOUR',
                                       StringType(), True),
                           StructField('Minimum',
                                       StringType(), True),
                           StructField('Maximum',
                                       StringType(), True),
                           StructField('Moyenne',
                                       StringType(), True),
                           StructField('Mediane',
                                       StringType(), True)])

    columns1 = StructType([StructField('TS',
                                       StringType(), True),
                           StructField('BPM',
                                       StringType(), True),
                           StructField('Date',
                                       StringType(), True)])

    list_file = [f for f in os.listdir('./Ressources/activities_heart_csv')]
    print(len(list_file[21:134]),list_file[21:134]) #Tom
    print(len(list_file[134:247]),list_file[134:247]) #Juan
    print(len(list_file[247:]),list_file[247:])#Erwan

    for file in tqdm(list_file[21:134]):
        df_full = spark.createDataFrame(data=[], schema=columns2)
        df = spark.read.csv(f'./Ressources/activities_heart_csv/{file}', schema=columns1)\
            .withColumn('HOUR', split(df['TS'], ':').getItem(0))
        df2 = df.groupBy("HOUR").agg(F.collect_list("BPM").alias("BPM"))
        df2.show()
        data_collect = df2.collect()
        for i in data_collect:
            l1 = i["BPM"]
            integer_map = map(int, l1)
            l1 = list(integer_map)
            # print(i["HOUR"], ":", l1)
            # print(statistics.median(l1), statistics.mean(l1), min(l1),max(l1))
            newRow = spark.createDataFrame([(i["HOUR"], min(l1), max(l1), statistics.mean(l1), statistics.median(l1))],columns2)
            df_full = df_full.union(newRow)
        # df_full.show()
        df_full.write.csv(f'./Ressources/stat_heart_csv/{file}')

stat_per_H()