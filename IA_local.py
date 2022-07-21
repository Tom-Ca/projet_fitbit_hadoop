import os
from datetime import datetime
from time import sleep
import tensorflow_io as tfio
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import tensorflow as tf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DataType, FloatType

from tensorflow import keras
from keras import layers
import keras
from tqdm import tqdm


def create_the_AI_training_dataset(file_in, file_out_pd, file_out_sk):
    spark = SparkSession.builder.getOrCreate()
    df_sp = spark.read.options(header=True).csv(f'Ressources/all_files_spark/{file_in}')
    df_sp = df_sp.withColumn("BPM", df_sp["value"].cast(IntegerType())) \
        .withColumn("date", F.to_date(df_sp["date"], "yyy-MM-dd")) \
        .withColumn("HOUR", df_sp["HOUR"].cast(IntegerType()))
    df_sp = df_sp.withColumn("TS", F.to_timestamp(F.concat(df_sp["date"], df_sp["time"]), "yyy-MM-ddHH:mm:ss"))

    dataset = df_sp.toPandas().sort_values(by=['TS'])
    dataset.reset_index(inplace=True, drop=True)

    j = 1
    while j <= 60:
        dataset["Min - " + str(j)] = np.nan
        j += 1

    for i in tqdm(dataset.index):
        j = 1
        tab = []
        time_1 = datetime.strptime(str(dataset["TS"][i]), "%Y-%m-%d %H:%M:%S")
        while j <= 60:
            if i - j > 0:
                time_2 = datetime.strptime(str(dataset["TS"][i - j]), "%Y-%m-%d %H:%M:%S")
                time_interval = int(round((time_1 - time_2).total_seconds() / 60))
                if time_interval == j:
                    dataset.at[i, "Min - " + str(j)] = dataset["BPM"][i - j]
            j += 1

    dataset = dataset.dropna()
    dataset.reset_index(inplace=True, drop=True)
    dataset = dataset.drop(["value", "date", "HOUR", "TS"], axis=1)
    dataset["time"] = dataset["time"].str.replace(":", "").astype(int)
    dataset['sport'] = dataset['sport'].astype(str)
    dataset = pd.get_dummies(dataset, columns=['sport'], prefix='', prefix_sep='')
    dataset.rename(columns={'0': 'Nothing'}, inplace=True)

    dataset.to_csv(path_or_buf=file_out_pd, index=False, sep=";")
    sparkDF = spark.createDataFrame(dataset)
    sparkDF.write.option("header", True).csv(f'Ressources/all_files_spark/{file_out_sk}')


def train_model(spark):
    df_sp = spark.read.options(header=True,inferSchema=True).csv(f'Ressources/all_files_spark/file_sport_for_IA.csv')
    df_sp.printSchema()
    dataset = df_sp.toPandas()
    print(dataset)
    train_dataset = dataset.sample(frac=0.8, random_state=0)
    test_dataset = dataset.drop(train_dataset.index)

    print(train_dataset.describe().transpose())

    train_features = train_dataset.copy()
    test_features = test_dataset.copy()
    train_labels = train_features.pop('BPM')
    test_labels = test_features.pop('BPM')

    normalizer = tf.keras.layers.Normalization(axis=-1)
    df_tmp = np.asarray(train_features).astype('float32')
    normalizer.adapt(df_tmp)
    # print(normalizer.mean.numpy())
    # first = np.array(train_features[:1])
    # with np.printoptions(precision=2, suppress=True):
    #     print('First example:', first)
    #     print()
    #     print('Normalized:', normalizer(first).numpy())

    # Construction du modèle séquentiel Keras

    dnn_model = keras.Sequential([
        normalizer,
        layers.Dense(128, activation='relu'),
        layers.Dense(128, activation='relu'),
        layers.Dense(1)
    ])

    dnn_model.compile(loss='mean_absolute_error', optimizer=tf.keras.optimizers.Adam(0.001))
    i = False
    while not i:
        history = dnn_model.fit(train_features, train_labels, validation_split=0.2, verbose=1, epochs=5)

        plt.plot(history.history['loss'], label='loss')
        plt.plot(history.history['val_loss'], label='val_loss')
        plt.ylim([0, 10])
        plt.xlabel('Epoch')
        plt.ylabel('Error [BPM]')
        plt.legend()
        plt.grid(True)
        plt.show()

        test_results = dnn_model.evaluate(test_features, test_labels, verbose=1)
        print(test_results)
        if test_results < 2.73:
            i = True

    dnn_model.save('Ressources/model_save1')

def use_model(spark):
    df_sp = spark.read.options(header=True, inferSchema=True, delimiter=";").csv(f'Ressources/file_for_test/test.csv')
    df_sp.printSchema()
    test = df_sp.toPandas()
    print(test)
    jeux_test = test.drop(columns=["BPM"])
    jeux_rep = test[["BPM","Nothing", "Circuit Training", "Martial Arts", "Run", "Sport", "Walk"]]
    print(test)
    model = keras.models.load_model('Ressources/model_save1')
    test_predictions = model.predict(jeux_test)
    jeux_rep["Predict BPM"] = test_predictions.round().astype(int)
    cols = jeux_rep.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    jeux_rep = jeux_rep[cols]
    jeux_rep.rename(columns={'BPM': 'Real BPM'}, inplace=True)
    print(jeux_rep)
    sparkDF = spark.createDataFrame(jeux_rep)
    sparkDF.write.option("header", True).csv(f'Ressources/file_for_test/reponse.csv')
