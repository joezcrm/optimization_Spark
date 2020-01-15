from pyspark.sql.types import (ArrayType,
                               DoubleType,
                               IntegerType,
                               StructField,
                               StructType)
from pyspark.sql.functions import udf, desc, col

def calculate_statistics(spark, solutions):
    """
    Return an ordered set of solutions, maximum value, and
        minimum value:
        result[0] is a list typed ordered solutions, with differences
        result[1] is the total difference
        result[2] is the maximum value
        result[3] is the minimum value
    :param spark, an instance of SparkSession
    :param solutions, a list typed solutions:
        solutions[][0] is solution_id
        solutions[][1] is list typed solution
        solutions[][2] is the value of the solution
    """
    solutions_schema = StructType([
        StructField("solution_id", IntegerType(), False),
        StructField("solution", ArrayType(DoubleType()), False),
        StructField("value", DoubleType(), False)
    ])
    # Calculate maximum and minimum
    solutions_df = spark.createDataFrame(solutions, solutions_schema)
    temp_df = solutions_df.selectExpr(["max(value) AS max_value", \
        "min(value) AS min_value"])
    temp = temp_df.first()
    max_value = temp[0]
    min_value = temp[1]
    
    # Add a difference column and reorder
    get_dif = udf(lambda x: max_value - x, DoubleType())
    dif_solutions_df = solutions_df.withColumn("dif", get_dif(col("value"))) \
        .select(["solution_id", "solution", "value", "dif"]) \
            .orderBy(desc("dif"))
    temp_df = dif_solutions_df.selectExpr(["sum(dif) AS sum_dif"])
    temp = temp_df.first()
    total = temp[0]
    dif_solutions = dif_solutions_df.collect()
    return [dif_solutions, total, max_value, min_value]