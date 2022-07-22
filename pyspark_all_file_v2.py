import os

from IA import create_the_AI_training_dataset, train_model, use_model
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType




# from tqdm import tqdm


def tab1(spark):
    print("tab 1")
    df = spark.read.option("multiline", "true").json(f"hdfs://localhost:19000/tomca/user/activities_heart/*.json")
    df2 = df.select(df["activities-heart"]["dateTime"].getItem(0).alias('date'),
                    F.explode(df["activities-heart-intraday"]["dataset"]).alias('vals')) \
        .select("vals.time", "vals.value", "date") \
        .withColumn('HOUR', F.split('time', ':').getItem(0))
    df2.show()

    df2.write.option("header", True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/BPM.csv')
    print("fichier d'entré : ")
    df.show()
    df.printSchema()

    print("fichier de sortie : ")
    df2.show()
    df2.printSchema()

def stat(spark):
    print("stat 1")
    df = spark.read.options(header=True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/BPM.csv')
    df = df.withColumn("value", df["value"].cast(IntegerType()))
    print("fichier d'entré : ")
    df.show()
    df.printSchema()

    try:
        df2 = df.groupBy("date") \
            .agg(F.min("value").alias("min_value"),
                 F.avg("value").alias("avg_value"),
                 F.max("value").alias("max_value"))
        print("fichier de sortie 1: ")
        df2.show()
        df2.printSchema()
        df2.write.option("header", True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/group_by_days.csv')

        df3 = df.groupBy("date", "HOUR") \
            .agg(F.min("value").alias("min_value"),
                 F.avg("value").alias("avg_value"),
                 F.max("value").alias("max_value"))
        print("fichier de sortie 2: ")
        df3.show()
        df3.printSchema()
        df3.write.option("header", True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/group_by_days_hour.csv')
    except:
        pass


def tab2_spark(spark):
    print("tab 2")
    df_init = spark.read.options(header=True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/BPM.csv')

    df = spark.read.option("multiline", "true").json(f"hdfs://localhost:19000/tomca/user/activity_log/*.json")
    print("fichier d'entré 1 : ")
    df_init.show()
    df_init.printSchema()
    print("fichier d'entré 2 : ")
    df.show()
    df.printSchema()

    df2 = df.select(df["originalDuration"].alias('Duration'), df["originalStartTime"].alias('StartTime'),
                    df["activityName"].alias('activityName')) \
        .withColumn('date', F.split('StartTime', 'T').getItem(0)) \
        .withColumn('HOUR', F.split('StartTime', 'T').getItem(1))
    data_collect = df2.collect()
    i = True
    for row in data_collect:
        date = row["date"]
        Duration = int(int(row["Duration"]) / 60000)
        activityName = row["activityName"]
        hour = row["HOUR"].split(":")
        HOUR = f"{hour[0]}:{hour[1]}:00"
        list_hour = pd.date_range(HOUR, periods=Duration, freq="1min").strftime('%H:%M:%S').to_list()
        df_not_empty = df_init.filter(df_init["date"].isin([date])).count() > 0
        if df_not_empty:
            # print('ok')
            if i:
                df_init = df_init.withColumn("sport",
                                             F.when(((df_init["date"] == date) & (df_init["time"].isin(list_hour))),
                                                    activityName)
                                             .otherwise("0"))
                i = False
            else:
                df_init = df_init.withColumn("sport",
                                             F.when(((df_init["date"] == date) & (df_init["time"].isin(list_hour))),
                                                    activityName)
                                             .when(df_init["sport"] != "0", df_init["sport"])
                                             .otherwise("0"))
    print("fichier de sortie : ")
    df_init.show()
    df_init.printSchema()
    df_init.write.option("header", True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/BPM_plus_sport.csv')


def tab2_pd(spark):
    print("tab 2")
    df_init = spark.read.options(header=True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/BPM.csv')
    dataset_init = df_init.toPandas()
    df = spark.read.option("multiline", "true").json(f"hdfs://localhost:19000/tomca/user/activity_log/*.json")

    print("fichier d'entré 1 : ")
    df_init.show()
    df_init.printSchema()
    print("fichier d'entré 2 : ")
    df.show()
    df.printSchema()

    df2 = df.select(df["originalDuration"].alias('Duration'), df["originalStartTime"].alias('StartTime'),
                    df["activityName"].alias('activityName')) \
        .withColumn('date', F.split('StartTime', 'T').getItem(0)) \
        .withColumn('HOUR', F.split('StartTime', 'T').getItem(1))
    dataset = df2.toPandas()
    dataset["sport"] = np.nan

    hour_all = {'time': [], 'date': [], 'sport': []}
    df_hour_all = pd.DataFrame(data=hour_all)
    for i in dataset.index:
        date = dataset["date"][i]
        Duration = int(int(dataset["Duration"][i]) / 60000)
        activityName = dataset["activityName"][i]
        hour = dataset["HOUR"][i].split(":")
        HOUR = f"{hour[0]}:{hour[1]}:00"
        df_hour = pd.date_range(HOUR, periods=Duration, freq="1min").strftime('%H:%M:%S').to_frame(index=False, name='time')
        df_hour["date"] = date
        df_hour["sport"] = activityName
        # print(date, Duration, activityName)
        # print(df_hour)
        df_hour_all = pd.concat([df_hour_all, df_hour])
    df_hour_all["TS"] = df_hour_all['date'].astype(str) + df_hour_all["time"].astype(str)
    dataset_init["TS"] = dataset_init['date'].astype(str) + dataset_init["time"].astype(str)
    df_hour_all = df_hour_all[["sport", "TS"]]
    df_cd = pd.merge(df_hour_all, dataset_init, how='right', on='TS')
    print(df_cd)
    df_cd['sport'] = df_cd['sport'].fillna("0")
    sparkDF = spark.createDataFrame(df_cd)
    print("fichier de sortie : ")
    sparkDF.show()
    sparkDF.printSchema()
    sparkDF.write.option("header", True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/BPM_plus_sport.csv')


def stat2(spark):
    print("stat 2")
    df = spark.read.options(header=True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/BPM_plus_sport.csv')
    df = df.withColumn("value", df["value"].cast(IntegerType()))
    print("fichier d'entré 1 : ")
    df.show()
    df.printSchema()

    df2 = df.groupBy("date") \
        .agg(F.min("value").alias("min_value"),
             F.avg("value").alias("avg_value"),
             F.max("value").alias("max_value"),
             F.max("sport").alias("sport"))
    print("fichier de sortie 1: ")
    df2.show()
    df2.printSchema()
    df2.write.option("header", True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/group_by_days_sport.csv')

    df3 = df.groupBy("date", "HOUR") \
        .agg(F.min("value").alias("min_value"),
             F.avg("value").alias("avg_value"),
             F.max("value").alias("max_value"),
             F.max("sport").alias("sport"))
    print("fichier de sortie 2: ")
    df3.show()
    df3.printSchema()
    df3.write.option("header", True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/group_by_days_hour_sport.csv')


def affichage2(spark):
    print("affichage 2")
    df = spark.read.options(header=True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/group_by_days_sport.csv')
    df = df.withColumn("max_value", df["max_value"].cast(IntegerType())) \
        .withColumn("min_value", df["min_value"].cast(IntegerType())) \
        .withColumn("avg_value", df["avg_value"].cast(FloatType())) \
        .withColumn("date", F.to_date(df["date"], "yyy-MM-dd"))
    pandasDF = df.toPandas().sort_values(by=['date'])
    pandasDF["sport"] = pandasDF['sport'].apply(lambda x: 200 if x != "0" else 0)
    plt.plot(pandasDF["date"], pandasDF["avg_value"], color='blue')
    plt.plot(pandasDF["date"], pandasDF["min_value"], color='green')
    plt.plot(pandasDF["date"], pandasDF["max_value"], color='red')
    plt.bar(pandasDF["date"], pandasDF["sport"], color='cyan')
    plt.ylim(20, 200)
    plt.show()

    df = spark.read.options(header=True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/group_by_days_hour_sport.csv')
    df = df.withColumn("max_value", df["max_value"].cast(IntegerType())) \
        .withColumn("min_value", df["min_value"].cast(IntegerType())) \
        .withColumn("avg_value", df["avg_value"].cast(FloatType())) \
        .withColumn("TS", F.to_timestamp(F.concat(df["date"], df["HOUR"]), "yyy-MM-ddHH"))
    pandasDF = df.toPandas().sort_values(by=['TS'])
    pandasDF["sport"] = pandasDF['sport'].apply(lambda x: 200 if x != "0" else 0)
    plt.plot(pandasDF["TS"], pandasDF["avg_value"], color='blue')
    plt.plot(pandasDF["TS"], pandasDF["min_value"], color='green')
    plt.plot(pandasDF["TS"], pandasDF["max_value"], color='red')
    plt.bar(pandasDF["TS"], pandasDF["sport"], color='cyan')
    # todo ecart max min
    plt.margins(x=0, y=-0.25)
    plt.ylim(20, 200)
    plt.show()

def affichage(spark):
    print("affichage 1")
    df = spark.read.options(header=True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/group_by_days_sport.csv')
    df = df.withColumn("max_value", df["max_value"].cast(IntegerType())) \
        .withColumn("min_value", df["min_value"].cast(IntegerType())) \
        .withColumn("avg_value", df["avg_value"].cast(FloatType())) \
        .withColumn("date", F.to_date(df["date"], "yyy-MM-dd"))
    pandasDF = df.toPandas().sort_values(by=['date'])
    plt.plot(pandasDF["date"], pandasDF["avg_value"], color='blue')
    plt.plot(pandasDF["date"], pandasDF["min_value"], color='green')
    plt.plot(pandasDF["date"], pandasDF["max_value"], color='red')

    plt.ylim(20, 200)
    plt.show()

    df = spark.read.options(header=True).csv(f'hdfs://localhost:19000/tomca/user/all_files_spark/group_by_days_hour_sport.csv')
    df = df.withColumn("max_value", df["max_value"].cast(IntegerType())) \
        .withColumn("min_value", df["min_value"].cast(IntegerType())) \
        .withColumn("avg_value", df["avg_value"].cast(FloatType())) \
        .withColumn("TS", F.to_timestamp(F.concat(df["date"], df["HOUR"]), "yyy-MM-ddHH"))
    pandasDF = df.toPandas().sort_values(by=['TS'])
    plt.plot(pandasDF["TS"], pandasDF["avg_value"], color='blue')
    plt.plot(pandasDF["TS"], pandasDF["min_value"], color='green')
    plt.plot(pandasDF["TS"], pandasDF["max_value"], color='red')
    # todo ecart max min
    plt.margins(x=0, y=-0.25)
    plt.ylim(20, 200)
    plt.show()

def affichage_day(spark):
    print("affichage day")

    empty = 1
    df = spark.read.options(header=True).csv(
        f'hdfs://localhost:19000/tomca/user/all_files_spark/group_by_days_hour_sport.csv')
    df = df.withColumn("max_value", df["max_value"].cast(IntegerType())) \
        .withColumn("min_value", df["min_value"].cast(IntegerType())) \
        .withColumn("avg_value", df["avg_value"].cast(FloatType())) \
        .withColumn("TS", F.to_timestamp(F.concat(df["date"], df["HOUR"]), "yyy-MM-ddHH"))
    while empty:
        print('\nEntrez une date de type : yyyy-mm-dd')
        x = input()
        df2 = df.filter(df.date == str(x))
        if df2.count() > 0:
            empty = 0
    pandasDF = df2.toPandas().sort_values(by=['TS'])
    pandasDF["sport"] = pandasDF['sport'].apply(lambda x: 200 if x != "0" else 0)
    plt.title("Données du " + pandasDF["date"][0])
    plt.plot(pandasDF["HOUR"], pandasDF["avg_value"], color='blue')
    plt.plot(pandasDF["HOUR"], pandasDF["min_value"], color='green')
    plt.plot(pandasDF["HOUR"], pandasDF["max_value"], color='red')
    plt.bar(pandasDF["HOUR"], pandasDF["sport"], color='cyan')
    # todo ecart max min
    plt.margins(x=0, y=-0.25)
    plt.ylim(20, 200)
    plt.show()


if __name__ == '__main__':

    # spark = SparkSession.builder.getOrCreate()
    os.environ["HADOOP_USER_NAME"] = "hdfs"
    os.environ["PYTHON_VERSION"] = "3.7.0"
    spark = SparkSession.builder.appName("test").getOrCreate()

    # tab1(spark)
    # stat(spark)
    # # tab2_spark(spark)
    # tab2_pd(spark)
    # stat2(spark)
    affichage(spark)
    affichage2(spark)
    affichage_day(spark)
    # create_the_AI_training_dataset("BPM_plus_sport.csv", "df_sport_for_ia2.csv", "file_sport_for_IA.csv")
    # train_model(spark)
    use_model(spark)

