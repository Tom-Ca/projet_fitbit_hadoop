from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql.types import IntegerType, FloatType

def tab1(spark):

    df = spark.read.option("multiline", "true").json(f"Ressources/activities_heart/2021-08-01.json")
    dfbis = spark.read.option("multiline", "true").json(f"Ressources/activities_heart/2022-05-27.json")
    dfbis2 = spark.read.option("multiline", "true").json(f"Ressources/activities_heart/2022-02-09.json")
    try:
        df2 = df.select(F.explode(df["activities-heart-intraday"]["dataset"]).alias('vals'))\
                .select("vals.time", "vals.value")\
                .withColumn('HOUR', F.split('time', ':').getItem(0)) \
                .withColumn("date", F.lit("2021-08-01"))
        # df2.show()
        # df2.printSchema()

        df3 = dfbis.select(F.explode(dfbis["activities-heart-intraday"]["dataset"]).alias('vals')) \
            .select("vals.time", "vals.value") \
            .withColumn('HOUR', F.split('time', ':').getItem(0)) \
            .withColumn("date", F.lit("2022-05-27"))

        df4 = dfbis2.select(F.explode(dfbis2["activities-heart-intraday"]["dataset"]).alias('vals')) \
            .select("vals.time", "vals.value") \
            .withColumn('HOUR', F.split('time', ':').getItem(0)) \
            .withColumn("date", F.lit("2022-02-09"))
        # df3.show()
        # df3.printSchema()
        df_full = df2.union(df3)
        df_full = df_full.union(df4)
        print("fichier d'entré : ")
        df.show()
        df.printSchema()

        print("fichier de sortie : ")
        df_full.show()
        df_full.printSchema()

        df_full.write.option("header", True).csv(f'./Ressources/files_test/BPM.csv')
    except:
        pass


def stat(spark):

    df = spark.read.options(header=True).csv(f'./Ressources/files_test/BPM.csv')
    df = df.withColumn("value", df["value"].cast(IntegerType()))
    print("fichier d'entré : ")
    df.show()
    df.printSchema()

    df2 = df.groupBy("date") \
        .agg(F.min("value").alias("min_value"),
             F.avg("value").alias("avg_value"),
             F.max("value").alias("max_value"))
    print("fichier de sortie 1: ")
    df2.show()
    df2.printSchema()

    df2.write.option("header", True).csv(f'./Ressources/files_test/group_by_days.csv')

    df3 = df.groupBy("date", "HOUR") \
        .agg(F.min("value").alias("min_value"),
             F.avg("value").alias("avg_value"),
             F.max("value").alias("max_value"))
    print("fichier de sortie 1: ")
    df3.show()
    df3.printSchema()

    df3.write.option("header", True).csv(f'./Ressources/files_test/group_by_days_hour.csv')


def tab2(spark):
    df_init = spark.read.options(header=True).csv(f'./Ressources/files_test/BPM.csv')

    df = spark.read.option("multiline", "true").json(f"Ressources/activity_log/2022-05-27_11h43m51s.json")
    dfbis = spark.read.option("multiline", "true").json(f"Ressources/activity_log/2021-08-01_19h42m42s.json")
    dfbis2 = spark.read.option("multiline", "true").json(f"Ressources/activity_log/2022-02-09_22h06m50s.json")
    print("fichier d'entré 1 : ")
    df_init.show()
    df_init.printSchema()
    print("fichier d'entré 2 : ")
    df.show()
    df.printSchema()
    try:
        df2 = df.select(df["originalDuration"].alias('Duration'),df["originalStartTime"].alias('StartTime'),df["activityName"].alias('activityName'))\
                .withColumn('date', F.split('StartTime', 'T').getItem(0)) \
                .withColumn('HOUR', F.split('StartTime', 'T').getItem(1))
        # df2.show()
        # df2.printSchema()
        df3 = dfbis.select(dfbis["originalDuration"].alias('Duration'), dfbis["originalStartTime"].alias('StartTime'),
                        dfbis["activityName"].alias('activityName')) \
            .withColumn('date', F.split('StartTime', 'T').getItem(0)) \
            .withColumn('HOUR', F.split('StartTime', 'T').getItem(1))

        df4 = dfbis2.select(dfbis2["originalDuration"].alias('Duration'), dfbis2["originalStartTime"].alias('StartTime'),
                           dfbis2["activityName"].alias('activityName')) \
            .withColumn('date', F.split('StartTime', 'T').getItem(0)) \
            .withColumn('HOUR', F.split('StartTime', 'T').getItem(1))

        date = df2.collect()[0]["date"]
        Duration = int(int(df2.collect()[0]["Duration"])/60000)
        activityName = df2.collect()[0]["activityName"]
        hour = df2.collect()[0]["HOUR"].split(":")
        HOUR = f"{hour[0]}:{hour[1]}:00"
        list_hour = pd.date_range(HOUR,periods=Duration, freq="1min").strftime('%H:%M:%S').to_list()
        df_init = df_init.withColumn("sport", F.when(((df_init["date"] == date)
                                                  & (df_init["time"].isin(list_hour)))
                                                 , activityName)
                                            .otherwise("0"))

        date = df3.collect()[0]["date"]
        Duration = int(int(df3.collect()[0]["Duration"])/60000)
        activityName = df3.collect()[0]["activityName"]
        hour = df3.collect()[0]["HOUR"].split(":")
        HOUR = f"{hour[0]}:{hour[1]}:00"
        list_hour = pd.date_range(HOUR,periods=Duration, freq="1min").strftime('%H:%M:%S').to_list()
        df_init = df_init.withColumn("sport", F.when(((df_init["date"] == date)
                                                  & (df_init["time"].isin(list_hour)))
                                                 , activityName)
                                            .when(df_init["sport"] != "0" ,df_init["sport"])
                                            .otherwise("0"))

        date = df4.collect()[0]["date"]
        Duration = int(int(df4.collect()[0]["Duration"]) / 60000)
        activityName = df4.collect()[0]["activityName"]
        hour = df4.collect()[0]["HOUR"].split(":")
        HOUR = f"{hour[0]}:{hour[1]}:00"
        list_hour = pd.date_range(HOUR, periods=Duration, freq="1min").strftime('%H:%M:%S').to_list()
        df_init = df_init.withColumn("sport", F.when(((df_init["date"] == date)
                                                    & (df_init["time"].isin(list_hour)))
                                                   , activityName)
                                   .when(df_init["sport"] != "0", df_init["sport"])
                                   .otherwise("0"))
        print("fichier de sortie : ")
        df_init.show()
        df_init.printSchema()
        df_init.write.option("header", True).csv(f'./Ressources/files_test/BPM_plus_sport.csv')

    except:
        pass

def stat2(spark):

    df = spark.read.options(header=True).csv(f'./Ressources/files_test/BPM_plus_sport.csv')
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
    df2.write.option("header", True).csv(f'./Ressources/files_test/group_by_days_sport.csv')

    df3 = df.groupBy("date", "HOUR") \
        .agg(F.min("value").alias("min_value"),
             F.avg("value").alias("avg_value"),
             F.max("value").alias("max_value"),
             F.max("sport").alias("sport"))
    print("fichier de sortie 2: ")
    df3.show()
    df3.printSchema()
    df3.write.option("header", True).csv(f'./Ressources/files_test/group_by_days_hour_sport.csv')

def affichage(spark):

    df = spark.read.options(header=True).csv(f'./Ressources/files_test/group_by_days_sport.csv')
    df = df.withColumn("max_value", df["max_value"].cast(IntegerType()))\
        .withColumn("min_value", df["min_value"].cast(IntegerType()))\
        .withColumn("avg_value", df["avg_value"].cast(FloatType()))\
        .withColumn("date", F.to_date(df["date"], "yyy-MM-dd"))
    pandasDF = df.toPandas().sort_values(by=['date'])
    pandasDF["sport"] = pandasDF['sport'].apply(lambda x: 200 if x != "0" else 0)
    plt.plot(pandasDF["date"], pandasDF["avg_value"],color='blue')
    plt.plot(pandasDF["date"], pandasDF["min_value"],color='green')
    plt.plot(pandasDF["date"], pandasDF["max_value"],color='red')
    plt.bar(pandasDF["date"], pandasDF["sport"],color='red')

    plt.ylim(20, 200)
    plt.show()


    df = spark.read.options(header=True).csv(f'./Ressources/files_test/group_by_days_hour_sport.csv')
    df = df.withColumn("max_value", df["max_value"].cast(IntegerType()))\
        .withColumn("min_value", df["min_value"].cast(IntegerType()))\
        .withColumn("avg_value", df["avg_value"].cast(FloatType())) \
        .withColumn("TS", F.to_timestamp(F.concat(df["date"], df["HOUR"]), "yyy-MM-ddHH"))
    pandasDF = df.toPandas().sort_values(by=['TS'])
    pandasDF["sport"] = pandasDF['sport'].apply(lambda x: 200 if x != "0" else 0)
    plt.plot(pandasDF["TS"], pandasDF["avg_value"],color='blue')
    plt.plot(pandasDF["TS"], pandasDF["min_value"],color='green')
    plt.plot(pandasDF["TS"], pandasDF["max_value"],color='red')
    plt.bar(pandasDF["TS"], pandasDF["sport"],color='red')

    plt.ylim(20, 200)
    plt.show()

def affichage2(spark):

    df = spark.read.options(header=True).csv(f'./Ressources/files_test/group_by_days.csv')
    print("fichier d'entré : ")
    df = df.withColumn("max_value", df["max_value"].cast(IntegerType()))\
        .withColumn("min_value", df["min_value"].cast(IntegerType()))\
        .withColumn("avg_value", df["avg_value"].cast(FloatType()))\
        .withColumn("date", F.to_date(df["date"], "yyy-MM-dd"))
    pandasDF = df.toPandas().sort_values(by=['date'])
    plt.plot(pandasDF["date"], pandasDF["avg_value"],color='blue')
    plt.plot(pandasDF["date"], pandasDF["min_value"],color='green')
    plt.plot(pandasDF["date"], pandasDF["max_value"],color='red')

    plt.ylim(20, 200)
    plt.show()


    df = spark.read.options(header=True).csv(f'./Ressources/files_test/group_by_days_hour.csv')
    df = df.withColumn("max_value", df["max_value"].cast(IntegerType()))\
        .withColumn("min_value", df["min_value"].cast(IntegerType()))\
        .withColumn("avg_value", df["avg_value"].cast(FloatType())) \
        .withColumn("TS", F.to_timestamp(F.concat(df["date"], df["HOUR"]), "yyy-MM-ddHH"))
    pandasDF = df.toPandas().sort_values(by=['TS'])
    plt.plot(pandasDF["TS"], pandasDF["avg_value"],color='blue')
    plt.plot(pandasDF["TS"], pandasDF["min_value"],color='green')
    plt.plot(pandasDF["TS"], pandasDF["max_value"],color='red')

    plt.ylim(20, 200)
    plt.show()


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # tab1(spark)
    # tab2(spark)
    # stat(spark)
    # stat2(spark)
    affichage(spark)
    affichage2(spark)

