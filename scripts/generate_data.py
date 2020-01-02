from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType,
                               StructField,
                               StructType)
from random import random, uniform

path = "s3://joezcrmdb/data/testing_data.parquet"

def main():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    data_length = 100
    data = []
    for num in range(data_length):
        x_axis = float(random())
        error = uniform(-0.1, 0.1)
        y_axis = 9 + 10*pow(x_axis, 1) - 11*pow(x_axis,2) + \
            0 * pow(x_axis, 3) + 21 * pow(x_axis, 4)
        y_axis = float(y_axis * (1 + error))
        data.append([x_axis, y_axis])
    schema = StructType([
        StructField("x_axis", DoubleType()),
        StructField("y_axis", DoubleType())      
    ])
    data_df = spark.createDataFrame(data, schema)
    data_df.write.mode("overwrite") \
        .parquet(path)

if __name__ == "__main__":
    main()
