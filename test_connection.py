import os
from pyspark.sql import SparkSession



if __name__ == '__main__':
    os.environ["HADOOP_USER_NAME"] = "hdfs"
    os.environ["PYTHON_VERSION"] = "3.7.0"
    sparkSession = SparkSession.builder.appName("test").getOrCreate()
    data = [('data 1', 1), ('data 2', 2), ('data 3', 3), ('data 4', 4)]
    df = sparkSession.createDataFrame(data)
    df.write.csv("hdfs://localhost:19000/tomca/user/example/example.csv")

# commende use
# hadoop fs -copyFromLocal C:/Users/tomca/Documents/ESGI/3IABD2/S2/hadoope/projet/Ressources/activities_heart /tomca/user/
# hadoop fs -copyFromLocal C:/Users/tomca/Documents/ESGI/3IABD2/S2/hadoope/projet/Ressources/activity_log /tomca/user/
# hadoop fs -mkdir /tomca/user/all_files_spark
# hadoop fs -copyFromLocal C:/Users/tomca/Documents/ESGI/3IABD2/S2/hadoope/projet/Ressources/all_files_spark/file_sport_for_IA.csv /tomca/user/all_files_spark/
# hadoop fs -mkdir /tomca/user/file_for_test
# hadoop fs -copyFromLocal C:/Users/tomca/Documents/ESGI/3IABD2/S2/hadoope/projet/Ressources/file_for_test/test.csv /tomca/user/file_for_test/
