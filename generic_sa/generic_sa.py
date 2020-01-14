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
from poly import (poly_generate as generate,
                             poly_initialize as initialize,
                             evaluate_all as evaluate)
from math import exp, log, sqrt
from random import random, seed

# Parquet data path
data_path = "data/testing_data.parquet"
# Result path
result_path = "output/results.parquet"
# Number of polinomial terms
solution_length = 6
# Number of solutions
population = 20
# Number of runs for each generation
runs = 60
# Initial accepting rate
accepting_rate = 0.99
# Terminating condition, in interval (0, 1), a small
# percentage error will lead to a long run time
percentage_error = 0.01
# Distance parameter, used to calculate the next control
# parameter, a positive number, a small parameter will
# lead to a long run time, but small error
distance_param = 0.01
# A positive multiplier on the standard deviation;
# a larger multiplier will lead to small error and long
# run time
dev_multiplier = 3
# The maximum steps to run; add to guarantee that the
# algorithm will terminate
max_steps = 200
# Run number of sa before selecting
select_interval = 10

def optimize(spark, solutions, control, data_df):
    """
    Return the resulting a list of solution_id, solution, and value
        result[][0] is solution_id;
        result[][1] is solution;
        result[][2] is value;
        result[][3] is new control parameter
    :param solutions, a list;
        solutions[][0] is the integer typed solution_id;
        solutions[][1] is a list representing a current solution;
        solutions[][2] is the value of a solution
    :param control, a double type representing the control parameter
    :param data_df, a spark DataFrame to be passed to calculate the
        value, representing the data needed for the calculation
    """ 
    length = len(solutions)
    
    v_sums = []
    v_sqr_sums = []
    for item in range(length):
        v_sums.append(0)
        v_sqr_sums.append(0)
        
    for run in range(runs):
        temp_solutions = []
        counter = 0
        while counter < length:
            temp_solutions.append([solutions[counter][0], generate(solutions[counter][1])])
            counter += 1
        
        # Evaluate temp solutions and replace the temp solutions
        temp_solutions = evaluate(spark, temp_solutions, data_df)
        
        new_solutions = []
        counter = 0
        while counter < length:

            if temp_solutions[counter][0] != solutions[counter][0]:
                raise ValueError("Solution ids mismatch")
            # Calculate statistics
            v_sums[counter] += temp_solutions[counter][2]
            v_sqr_sums[counter] += pow(temp_solutions[counter][2], 2)
            
            # If the generated new value is smaller than the current value,
            # accept it as a solution
            if temp_solutions[counter][2] < solutions[counter][2]:
                new_solutions.append([solutions[counter][0], temp_solutions[counter][1], \
                    temp_solutions[counter][2]])
            # If the generated new value is larger than the current one
            # but satisfies the criteria for simulated annealing,
            # the new solution is also accepted
            else:
                seed()
                rand_num = random()
                threshold = exp((solutions[counter][2] - temp_solutions[counter][2]) \
                    /control)
                if rand_num < threshold:
                    new_solutions.append([solutions[counter][0], \
                        temp_solutions[counter][1], temp_solutions[counter][2]])
                else:
                    new_solutions.append([solutions[counter][0], \
                        solutions[counter][1], solutions[counter][2]])
            counter += 1
            
        solutions = new_solutions 
    
    counter = 0
    temp_solutions = []
    while counter < length:
        # Calculate standard deviation
        std_dev = sqrt(v_sqr_sums[counter]/runs - pow(v_sums[counter]/runs, 2))
        # Calculate the next ctrl parameter        
        ctrl_param = control/(1 + control * \
            log(1 + distance_param)/dev_multiplier/std_dev)
        temp_solutions.append([solutions[counter][0], solutions[counter][1], \
            solutions[counter][2], ctrl_param])
        counter += 1
        
    return temp_solutions

def calculate_param(spark, solutions, data_df):
    """
    Return a control parameter based on solution and value
    :param spark, an instance of SparkSession
    :param solutions, a list;
        solutions[][0] is the integer typed solution_id;
        solutions[][1] is a list representing a current solution;
        solutions[][2] is the value of a solution
    :param data_df, a spark DataFrame to be passed to calculate the
        value, representing the data needed for the calculation
    """
    length = len(solutions)
    # Reconstruct solution list and counter and total increase column
    test_solutions = []
    counter = 0
    while counter < length:
        test_solutions.append([solutions[counter][0], solutions[counter][1],
            solutions[counter][2], 0, 0.0])
        counter += 1
    solutions = test_solutions
    
    for run in range(runs):
        temp_solutions = []
        counter = 0
        while counter < length:
            temp_solutions.append([solutions[counter][0], generate(solutions[counter][1])])
            counter += 1
        # Evaluate solutions
        temp_solutions = evaluate(spark, temp_solutions, data_df)
        
        counter = 0
        new_solutions = []
        while counter < length:
            if temp_solutions[counter][0] != solutions[counter][0]:
                raise ValueError("Solution ids mismatch")
            if temp_solutions[counter][2] > solutions[counter][2]:
                new_solutions.append([solutions[counter][0], temp_solutions[counter][1], \
                    temp_solutions[counter][2], solutions[counter][3] + 1, \
                    solutions[counter][4] + temp_solutions[counter][2] - solutions[counter][2]])
            else:
                new_solutions.append([solutions[counter][0], temp_solutions[counter][1], \
                    temp_solutions[counter][2], solutions[counter][3], \
                    solutions[counter][4]])
            counter += 1
            
        solutions = new_solutions
    
    # Calculate control parameters
    params = []
    counter = 0
    while counter < length:
        ctrl_param = (solutions[counter][4]/solutions[counter][3]) / \
            log(solutions[counter][3]/(solutions[counter][3] * accepting_rate \
            - (runs - solutions[counter][3]) * (1 -accepting_rate)))
        params.append(ctrl_param)
        counter += 1
    return params


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
    solutions = []
    counter = 0
    while counter < population:
        solutions.append([counter, initialize(solution_length)])
        counter += 1
    solutions = evaluate(spark, solutions, data_df)
    params = calculate_param(spark, solutions, data_df)
    
    counter = 0
    ctrl_param = 0
    while counter < len(params):
        if params[counter] > ctrl_param:
            ctrl_param = params[counter]
        counter += 1

    solutions_schema = StructType([
        StructField("solution_id", IntegerType(), False),
        StructField("solution", ArrayType(DoubleType()), False),
        StructField("value", DoubleType(), False)
    ])
    to_continue = True
    checker = 0
    while to_continue and checker < max_steps:
        for item in range(select_interval):
            # Generate new solutions based on the current solutions
            new_solutions = optimize(spark, solutions, ctrl_param, data_df)
            # Calcualte control parameter
            control_max = new_solutions[0][3]
            for individual in range(population):
                if new_solutions[individual][3] > control_max:
                    control_max = new_solutions[individual][3]                            
            ctrl_param = control_max
            solutions = []
            counter = 0
            while counter < population:
                solutions.append([new_solutions[counter][0], \
                    new_solutions[counter][1], new_solutions[counter][2]])
                counter += 1
        
        # Calculate average value, max value, and get the maximum control
        # as new control
        value_max = new_solutions[0][2]
        value_min = new_solutions[0][2]
        for individual in range(population):
            if new_solutions[individual][2] > value_max:
                value_max = new_solutions[individual][2]
            if new_solutions[individual][2] < value_min:
                value_min = new_solutions[individual][2]
        
        if value_max - value_min < abs(percentage_error * value_min):
            to_continue = False
        
        # Calculate difference and reorder list
        dif_solutions = []
        total_dif = 0
        counter = 0
        while counter < population:
            dif_min = value_max - new_solutions[0][2]
            dif_position = 0
            dif_length = len(new_solutions)
            for item in range(dif_length):
                if dif_min > (value_max - new_solutions[item][2]):
                    dif_min = value_max - new_solutions[item][2]
                    dif_position = item
            dif_solutions.append([counter, new_solutions[dif_position][1],\
                new_solutions[dif_position][2], dif_min])
            total_dif += dif_min
            new_solutions.pop(dif_position)
            counter += 1
        
        # Generate a new list of solutions
        counter = 0
        solution_id = 0
        leftover = population
        next_generation = []
        while counter < population:
            if counter != population -1:
                walker = 0
                while walker < (dif_solutions[counter][3]/total_dif*population) -1:
                    next_generation.append([solution_id, \
                        dif_solutions[counter][1], dif_solutions[counter][2]])
                    solution_id += 1
                    walker += 1
                leftover = leftover - walker
            else:
                for item in range(leftover):
                    next_generation.append([solution_id, \
                        dif_solutions[counter][1], dif_solutions[counter][2]])
                    solution_id += 1
            counter += 1
        if len(next_generation) != population:
            raise ValueError("Population mismacth")
        solutions = next_generation           
        checker += 1
     
    solutions_df = spark.createDataFrame(solutions, solutions_schema)        
    solutions_df.write.mode("overwrite").parquet(result_path)
    print(checker)
    
if __name__ == "__main__":
    main()
        
    