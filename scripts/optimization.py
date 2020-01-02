from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, col, lit, udf
from pyspark.sql.types import (ArrayType,
                               DoubleType,
                               IntegerType,
                               MapType,
                               StringType,
                               StructField,
                               StructType)
from sa_poly import (poly_evaluate as evaluate,
                     poly_generate as generate,
                     poly_initialize as initialize)
from math import exp, log
from random import random

# Parquet data path
data_path = "s3://joezcrmdb/data/testing_data.parquet"
# Result path
result_path = "s3://joezcrmdb/output/results.parquet"
# Data schema
parquet_schema = {"x_axis":"x_axis", "y_axis":"y_axis"}
# Number of runs for each generation
runs = 80
# Initial accepting rate
accepting_rate = 0.95
# Size of population
population = 20
# Terminating condition, in interval (0, 1), a small
# percentage error will lead to a long run time
percentage_error = 0.1
# Distance parameter, used to calculate the next control
# parameter, a positive number, a small parameter will
# lead to a long run time, but small error
distance_param = 0.05
# A positive multiplier on sigma, the standard deviation;
# a larger multiplier will lead to small error and long
# run time
sigma_multiplier = 3

def optimize(spark, solution, value, control, data_df, data_schema):
    """
    Return the next accepted solution using simulated annealing
    :param spark, an instance of SparkSession
    :param solution, a list representing the current solution
    :param value, a double type representing the current value
    :param control, a double type representing the control parameter
    :param data_df, a spark DataFrame to be passed to calculate the
        value, representing the data needed for the calculation
    :param data_schema, a dictionary representing the schema, where
        data_schema["x_axis"] is the column name of x-axis and
        data_schema["y_axis"] is the column name of y-axis
    """      
    for trial in range(runs):
        # Generate new solution
        temp_solution = generate(solution)
        # Calculate the value of the new solution
        temp_value = evaluate(spark, temp_solution, data_df, data_schema)
        # If the generated new value is smaller than the current value,
        # accept it as a solution
        if temp_value <= value:
            solution = temp_solution
            value = temp_value
        else:
            # If the generated new value is larger than the current one
            # but satisfies the criteria for simulated annealing,
            # the new solution is also accepted
            rand_num = random()
            threshold = exp((value - temp_value)/control)
            if rand_num < threshold:
                solution = temp_solution
                value = temp_value     
    return [solution, value]

def calculate_param(spark, solution, value, data_df, data_schema):
    """
    Return a control parameter based on solution and value
    :param spark, an instance of SparkSession
    :param solution, a list representing the current solution
    :param value, a double type representing the current value  
    :param data_df, a spark DataFrame to be passed to calculate the
        value, representing the data needed for the calculation
    :param data_schema, a dictionary representing the schema, where
        data_schema["x_axis"] is the column name of x-axis and
        data_schema["y_axis"] is the column name of y-axis
    """
    results = []
    for trial in range(runs):
        # Generate new solution
        temp_solution = generate(solution)
        # Calculate the value of the new solution
        temp_value = evaluate(spark, temp_solution, data_df, data_schema)
        # If the generated value is larger than the current one,
        # record the difference
        if temp_value > value:
            results.append(temp_value - value)
        else:
            results.append(None)
        value = temp_value
        solution = temp_solution
    
    # Counter the number of increase
    # Sum up the total increase
    counter = 0
    total_increase = 0
    for result in results:
        if result is not None:
            counter += 1
            total_increase += result
    if counter == 0:
        error_message = "Control parameter initializaion fails. " + \
            "The current solution is at its local maximum."
        raise ValueError(error_message)
    else:
        # Calculate the control parameter
        control_param = (total_increase/counter)/log(counter/ \
            (counter*accepting_rate - (runs-counter)*(1-accepting_rate)))
    return float(control_param)

def main():
    # Create an instance of SparkSession
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    # Read data
    data_df = spark.read.parquet(data_path)
    # Initialize data schema
    data_schema = parquet_schema
    # Initializing a set of solutions
    # To be modified for every problem
    length = 6
    solutions = []
    for individual in range(population):
        initial = initialize(length)
        value = evaluate(spark, initial, data_df, data_schema)
        param = calculate_param(spark, initial, value, data_df, data_schema)
        solutions.append([initial, value, param])
    # Create temporary schema for solutions
    solutions_schema = StructType([
        StructField("solution", ArrayType(DoubleType(), False), False),
        StructField("value", DoubleType(), False),
        StructField("temp_param", DoubleType(), False)
        ])
    # End initializing
    
    # Create a spark DataFrame
    solutions_df = spark.createDataFrame(solutions, solutions_schema)
    # Calculate statistics
    temp_df = solutions_df.selectExpr(["max(temp_param) as param", \
        "max(value) as max_value", "min(value) as min_value"])
    temp = temp_df.first()
    ctrl_param = temp[0]
    max_value = temp[1]
    min_value = temp[2]

    #for test in range(10):
    while (max_value - min_value) / min_value > percentage_error:
        # Generate new solutions based on the current solutions
        temp_solutions = []
        for individual in range(population):
            temp_solutions.append(optimize(spark, solutions[individual][0], \
                solutions[individual][1], ctrl_param, data_df, data_schema))
        # Schema for new solutions dataframe
        schema = StructType([
            StructField("solution", ArrayType(DoubleType(), False), False),
            StructField("value", DoubleType(), False),
            ])
        # Create a spark DataFrame
        solutions_df = spark.createDataFrame(temp_solutions, schema)
        # Calculate statistics
        temp_df = solutions_df.selectExpr(["max(value) as max_value",
            "min(value) as min_value", "stddev(value) as sigma"])
        temp = temp_df.first()
        max_value = temp[0]
        min_value = temp[1]
        sigma = temp[2]
        # Calculate the new control parameter
        ctrl_param = float(ctrl_param / (1 + \
            ctrl_param * log(1+ distance_param)/sigma_multiplier/sigma))
        # Calculate difference
        get_dif = udf(lambda x: max_value - x, DoubleType())
        solutions_df = solutions_df.withColumn("dif", get_dif(col("value"))) \
            .select(["solution", "value", "dif"]) \
            .orderBy(asc("dif"))
        temp_df = solutions_df.selectExpr(["sum(dif) as total"])
        temp = temp_df.first()
        total = temp[0]
        
        # Collect results to be processed
        solutions = solutions_df.collect()        
        # Generate a population of new solutions based on generic algorithm
        counter = 0
        new_solutions = []
        leftover = population
        while counter < population:           
            if (counter != population -1):
                walker = 0
                while walker < (solutions[counter][2]/total) \
                                * population - 1:
                    new_solutions.append([solutions[counter][0],
                        solutions[counter][1]])
                    walker += 1
                leftover = leftover - walker
            else:
                for num in range(leftover):
                    new_solutions.append([solutions[counter][0],
                        solutions[counter][1]])                   
            counter += 1
        
        # The population of new generation has to be identical as that
        # of the original
        if len(new_solutions) != population:
            raise ValueError("Population mismatch")
        # Assign the next list of solutions
        solutions = new_solutions
    # Write results to file    
    solutions_df.select(["solution", "value"]) \
        .write.mode("overwrite").parquet(result_path)
    
if __name__ == "__main__":
    main()
        
    
    
        
        
