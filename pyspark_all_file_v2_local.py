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
    # df = spark.read.option("multiline", "true").json(f"Ressources/activities_heart/2021-08-01.json")
    df = spark.read.option("multiline", "true").json(f"Ressources/activities_heart/*.json")
    df.printSchema()
    try:
        df2 = df.select(df["activities-heart"]["dateTime"].getItem(0).alias('date'),
                        F.explode(df["activities-heart-intraday"]["dataset"]).alias('vals')) \
            .select("vals.time", "vals.value", "date") \
            .withColumn('HOUR', F.split('time', ':').getItem(0))
        df2.show()

        df2.write.option("header", True).csv(f'./Ressources/all_files_spark/BPM.csv')
    except:
        pass

def stat(spark):
    df = spark.read.options(header=True).csv(f'./Ressources/all_files_spark/BPM.csv')
    df = df.withColumn("value", df["value"].cast(IntegerType()))
    df.printSchema()
    df.show()

    try:
        df2 = df.groupBy("date") \
            .agg(F.min("value").alias("min_value"),
                 F.avg("value").alias("avg_value"),
                 F.max("value").alias("max_value"))
        df2.printSchema()
        df2.show()
        df2.write.option("header", True).csv(f'./Ressources/all_files_spark/group_by_days.csv')

        df3 = df.groupBy("date", "HOUR") \
            .agg(F.min("value").alias("min_value"),
                 F.avg("value").alias("avg_value"),
                 F.max("value").alias("max_value"))
        df3.printSchema()
        df3.show()
        df3.write.option("header", True).csv(f'./Ressources/all_files_spark/group_by_days_hour.csv')
    except:
        pass


def tab2_spark(spark):
    df_init = spark.read.options(header=True).csv(f'./Ressources/all_files_spark/BPM.csv')

    df = spark.read.option("multiline", "true").json(f"Ressources/activity_log/*.json")

    # try:
    df2 = df.select(df["originalDuration"].alias('Duration'), df["originalStartTime"].alias('StartTime'),
                    df["activityName"].alias('activityName')) \
        .withColumn('date', F.split('StartTime', 'T').getItem(0)) \
        .withColumn('HOUR', F.split('StartTime', 'T').getItem(1))
    df2.show()
    df2.printSchema()
    data_collect = df2.collect()
    i = True
    for row in data_collect:
        date = row["date"]
        Duration = int(int(row["Duration"]) / 60000)
        activityName = row["activityName"]
        hour = row["HOUR"].split(":")
        HOUR = f"{hour[0]}:{hour[1]}:00"
        list_hour = pd.date_range(HOUR, periods=Duration, freq="1min").strftime('%H:%M:%S').to_list()
        print(date, Duration, activityName, list_hour)
        df_not_empty = df_init.filter(df_init["date"].isin([date])).count() > 0
        print(df_not_empty, type(df_not_empty))
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
        # else:
        #     print("no ok")
    df_init.show()
    df_init.printSchema()

    df_init.write.option("header", True).csv(f'./Ressources/all_files_spark/BPM_plus_sport.csv')

    # except:
    #     pass

def tab2_pd(spark):
    df_init = spark.read.options(header=True).csv(f'./Ressources/all_files_spark/BPM.csv')
    dataset_init = df_init.toPandas()
    df = spark.read.option("multiline", "true").json(f"Ressources/activity_log/*.json")

    df2 = df.select(df["originalDuration"].alias('Duration'), df["originalStartTime"].alias('StartTime'),
                    df["activityName"].alias('activityName')) \
        .withColumn('date', F.split('StartTime', 'T').getItem(0)) \
        .withColumn('HOUR', F.split('StartTime', 'T').getItem(1))
    dataset = df2.toPandas()
    print(dataset)
    print(dataset_init)
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
    print(df_hour_all)
    df_hour_all["TS"] = df_hour_all['date'].astype(str) + df_hour_all["time"].astype(str)
    dataset_init["TS"] = dataset_init['date'].astype(str) + dataset_init["time"].astype(str)
    df_hour_all = df_hour_all[["sport", "TS"]]
    print(df_hour_all)
    df_cd = pd.merge(df_hour_all, dataset_init, how='right', on='TS')
    print(df_cd)
    df_cd['sport'] = df_cd['sport'].fillna("0")
    sparkDF = spark.createDataFrame(df_cd)
    sparkDF.write.option("header", True).csv(f'./Ressources/all_files_spark/BPM_plus_sport.csv')


def stat2(spark):
    df = spark.read.options(header=True).csv(f'./Ressources/all_files_spark/BPM_plus_sport.csv')
    df = df.withColumn("value", df["value"].cast(IntegerType()))
    df.printSchema()
    df.show()

    df2 = df.groupBy("date") \
        .agg(F.min("value").alias("min_value"),
             F.avg("value").alias("avg_value"),
             F.max("value").alias("max_value"),
             F.max("sport").alias("sport"))

    df2.printSchema()
    df2.show()
    df2.write.option("header", True).csv(f'./Ressources/all_files_spark/group_by_days_sport.csv')

    df3 = df.groupBy("date", "HOUR") \
        .agg(F.min("value").alias("min_value"),
             F.avg("value").alias("avg_value"),
             F.max("value").alias("max_value"),
             F.max("sport").alias("sport"))
    df3.printSchema()
    df3.show()
    df3.write.option("header", True).csv(f'./Ressources/all_files_spark/group_by_days_hour_sport.csv')


def affichage(spark):
    df = spark.read.options(header=True).csv(f'./Ressources/all_files_spark/group_by_days_sport.csv')
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

    print(pandasDF)
    plt.ylim(20, 200)
    plt.show()

    df = spark.read.options(header=True).csv(f'./Ressources/all_files_spark/group_by_days_hour_sport.csv')
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
    print(pandasDF)
    plt.ylim(20, 200)
    plt.show()





if __name__ == '__main__':

    # spark = SparkSession.builder.getOrCreate()
    os.environ["HADOOP_USER_NAME"] = "hdfs"
    os.environ["PYTHON_VERSION"] = "3.7.0"
    spark = SparkSession.builder.appName("test").getOrCreate()

    tab1(spark)
    stat(spark)
    # tab2_spark(spark)
    tab2_pd(spark)
    stat2(spark)
    affichage(spark)
    # create_the_AI_training_dataset("BPM_plus_sport.csv", "df_sport_for_ia2.csv", "file_sport_for_IA.csv")
    train_model(spark)
    use_model()

