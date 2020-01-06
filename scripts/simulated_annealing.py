from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, col, lit, udf
from pyspark.sql.functions import array
from pyspark.sql.types import (ArrayType,
                               DoubleType,
                               IntegerType,
                               MapType,
                               StringType,
                               StructField,
                               StructType)
from sa_poly import (poly_generate as generate,
                     poly_initialize as initialize)
from math import exp, log, sqrt
from random import random

# Parquet data path
data_path = "s3://joezcrmdb/data/testing_data.parquet"
# Result path
result_path = "s3://joezcrmdb/output/results.parquet"
# Number of polinomial terms
solution_length = 6
# Number of runs for each generation
runs = 50
# Initial accepting rate
accepting_rate = 0.95
# Terminating condition, in interval (0, 1), a small
# percentage error will lead to a long run time
percentage_error = 0.05
# Distance parameter, used to calculate the next control
# parameter, a positive number, a small parameter will
# lead to a long run time, but small error
distance_param = 0.05
# A positive multiplier on the standard deviation;
# a larger multiplier will lead to small error and long
# run time
dev_multiplier = 3
# The maximum steps to run; add to guarantee that the
# algorithm will terminate
max_steps = 150
# The minimum steps to run;
min_steps = 30
# The steps used to make the value smooth and to calculate
# the termination criteria
smooth_steps = 5

@udf (DoubleType())
def evaluate(solution, x_axis, y_axis):
    """
    Evaluate a solution and return a DoubleType value
    :param solution, a list or tuple (a0, a1, a2, ...), representing
        the parameters of y = a0 + a1*pow(x, 1) + a2*pow(x,2) + ...
        The order is sensative in calculating the value, so that first
        element of "solution" should be the parameter associating with
        power 0, second associating with power 1, and so on
    :param x_axis, DoubleType, a value on x_axis column
    :param y_axis, DoubleType, a value on y_axis_column
    """    
    counter = 0
    result = 0
    while counter < len(solution):
        result += solution[counter]*pow(x_axis, counter)
        counter += 1
    return abs(y_axis - result)

def optimize(spark, solution, control, data_df):
    """
    Return the resulting a list of new solution, new value, and 
        the standard deviation of current step;
        result[0] is solution;
        result[1] is value;
        result[2] is standard deviation
    :param solution, a list representing the current solution
    :param control, a double type representing the control parameter
    :param data_df, a spark DataFrame to be passed to calculate the
        value, representing the data needed for the calculation
    """ 
    solution_schema = StructType([
        StructField("solution", ArrayType(DoubleType()), False)
    ])
    solution_df = spark.createDataFrame([[solution]], solution_schema)
    solution_df = data_df.crossJoin(solution_df)
    # Evaluate each data point
    value_df = solution_df.withColumn("value", \
        evaluate(col("solution"), col("x_axis"), col("y_axis")))
    # Sum up all values
    temp_df = value_df.selectExpr("SUM(value)")
    temp = temp_df.first()
    value = temp[0]
    # Are used to calculated standard deviation for those accepted
    value_sum = 0
    v_sqr_sum = 0
    counter = 0
    for trial in range(runs):
        # Generate new solution
        temp_solution = generate(solution)
        # Add solution column
        solution_df = spark.createDataFrame([[temp_solution]], solution_schema)
        solution_df = data_df.crossJoin(solution_df)
        value_df = solution_df.withColumn("value", \
            evaluate(col("solution"), col("x_axis"), col("y_axis")))
        temp_df = value_df.selectExpr("SUM(value)")
        temp = temp_df.first()
        temp_value = temp[0]
        # If the generated new value is smaller than the current value,
        # accept it as a solution
        if temp_value <= value:
            solution = temp_solution
            value = temp_value
            value_sum += temp_value
            v_sqr_sum += pow(temp_value, 2)
            counter += 1
        else:
            # If the generated new value is larger than the current one
            # but satisfies the criteria for simulated annealing,
            # the new solution is also accepted
            rand_num = random()
            threshold = exp((value - temp_value)/control)
            if rand_num < threshold:
                solution = temp_solution
                value = temp_value
                value_sum += temp_value
                v_sqr_sum += pow(temp_value, 2)
                counter += 1
    if counter == 0:
        std_dev = 0
    else:
        std_dev = sqrt(v_sqr_sum/counter - pow(value_sum/counter, 2))
    return [solution, value, std_dev]

def calculate_param(spark, solution, data_df):
    """
    Return a control parameter based on solution and value
    :param spark, an instance of SparkSession
    :param solution, a list representing the current solution
    :param value, a double type representing the current value  
    :param data_df, a spark DataFrame to be passed to calculate the
        value, representing the data needed for the calculation
    """
    solution_schema = StructType([
        StructField("solution", ArrayType(DoubleType()), False)
    ])
    solution_df = spark.createDataFrame([[solution]], solution_schema)
    solution_df = data_df.crossJoin(solution_df)
    value_df = solution_df.withColumn("value", \
        evaluate(col("solution"), col("x_axis"), col("y_axis")))
    temp_df = value_df.selectExpr("SUM(value)")
    temp = temp_df.first()
    value = temp[0]
    results = []
    for trial in range(runs):
        # Generate new solution
        temp_solution = generate(solution)
        solution_df = spark.createDataFrame([[temp_solution]], solution_schema)
        solution_df = data_df.crossJoin(solution_df)
        value_df = solution_df.withColumn("value", \
            evaluate(col("solution"), col("x_axis"), col("y_axis")))
        temp_df = value_df.selectExpr("SUM(value)")
        temp = temp_df.first()
        temp_value = temp[0]
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
        ctrl_param = (total_increase/counter)/log(counter/ \
            (counter*accepting_rate - (runs-counter)*(1-accepting_rate)))
    return float(ctrl_param)

def get_slope(v_list):
    """
    Return a slope based on a list;
    :param v_list, a list of dictionaries, 
        v_list[]["value"] represents the value,
        v_list[]["ctrl"] represents the control parameter
    """
    list_length = len(v_list)
    value_sum = 0
    ctrl_sum = 0
    vc_product_sum = 0
    ctrl_sqr_sum = 0
    for item in range(list_length):
        value_sum += v_list[item]["value"]
        ctrl_sum += v_list[item]["ctrl"]
        vc_product_sum += v_list[item]["value"]*v_list[item]["ctrl"]
        ctrl_sqr_sum += v_list[item]["ctrl"]*v_list[item]["ctrl"]
    value_avg = value_sum/list_length
    ctrl_avg = ctrl_sum/list_length
    vc_product_avg = vc_product_sum/list_length
    ctrl_sqr_avg = ctrl_sqr_sum/list_length
    slope = (value_avg*ctrl_avg - vc_product_avg)/(pow(ctrl_avg, 2) - ctrl_sqr_avg)
    return slope


def main():
    # Create an instance of SparkSession
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    # Read data
    data_df = spark.read.parquet(data_path)
    # Initializing a set of solutions
    # To be modified for every problem
    solution = initialize(solution_length)
    ctrl_param = calculate_param(spark, solution, data_df)
    
    solutions = []
    to_continue = True
    counter = 0
    while to_continue and counter < max_steps:
        # Generate new solutions based on the current solutions
        new_solution = optimize(spark, solution, ctrl_param, data_df)
        # Append solutions to a list in order to write all the results
        solutions.append([new_solution[0], new_solution[1], new_solution[2],\
            ctrl_param, counter])
        if counter > min_steps:
            list_length = len(solutions)
            smooth_list = []
            for outter_item in range(smooth_steps):
                # Use average of a few previous data points to smooth
                sm_value_sum = 0
                for inner_item in range(smooth_steps):
                    sm_value_sum += solutions[list_length - 1 \
                        - outter_item - inner_item][1]
                # Create lists to calculate slope
                smooth_list.append({"value": sm_value_sum/smooth_steps,
                    "ctrl":solutions[list_length -1 - outter_item][3]})
            sm_error = ctrl_param/smooth_list[smooth_steps-1]["value"]\
                * get_slope(smooth_list)
            if new_solution[2] == 0 or sm_error < percentage_error:
                to_continue = False 
        # Calculate the next ctrl parameter
        ctrl_param = ctrl_param/(1 + ctrl_param * \
            log(1 + distance_param)/dev_multiplier/new_solution[2])
        solution = new_solution[0]
        counter += 1
     
    # Write the solutions to file
    schema = StructType([
        StructField("solution", ArrayType(DoubleType())),
        StructField("value", DoubleType()),
        StructField("std_dev", DoubleType()),
        StructField("control", DoubleType()),
        StructField("run_number", IntegerType())
    ])
    solution_df = spark.createDataFrame(solutions, schema)        
    solution_df.write.mode("overwrite").parquet(result_path)
    
if __name__ == "__main__":
    main()
        
    
    
        
        
