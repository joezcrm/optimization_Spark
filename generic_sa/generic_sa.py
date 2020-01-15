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
from test_poly import (poly_generate as generate,
                             poly_initialize as initialize,
                             evaluate_all as evaluate)
from statistics_python import calculate_statistics
from math import exp, log, sqrt
from random import random, seed, randint

# Parquet data path
data_path = "data/testing_data.parquet"
# Result path
result_path = "output/results.parquet"
# Number of polinomial terms
solution_length = 1
# Number of solutions
population = 80
# Number of runs for each generation
runs = 20
# Number of runs to calculate initial control parameter
test_runs = 40
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
max_steps = 400
# The minimum steps to run
min_steps = 200
# The maximum percentage of the population for the largest
# to reproduce
ceilings = [0.20, 0.15, 0.10, 0.05]

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
    results = solutions
    solution_id = length
    
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
                results.append([solution_id, temp_solutions[counter][1], \
                    temp_solutions[counter][2]])
                solution_id += 1
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
                    results.append([solution_id, temp_solutions[counter][1], \
                        temp_solutions[counter][2]])
                    solution_id += 1
                else:
                    new_solutions.append([solutions[counter][0], \
                        solutions[counter][1], solutions[counter][2]])
            counter += 1           
        solutions = new_solutions 
    
    counter = 0
    ctrl_max = 0
    while counter < length:
        # Calculate standard deviation
        std_dev = sqrt(v_sqr_sums[counter]/runs - pow(v_sums[counter]/runs, 2))
        # Calculate the next ctrl parameter        
        ctrl_param = control/(1 + control * \
            log(1 + distance_param)/dev_multiplier/std_dev)
        if ctrl_max < ctrl_param:
            ctrl_max = ctrl_param
        counter += 1
        
    return [results, ctrl_max]

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
    
    for run in range(test_runs):
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
    counter = 0
    ctrl_max = 0
    while counter < length:
        ctrl_param = (solutions[counter][4]/solutions[counter][3]) / \
            log(solutions[counter][3]/(solutions[counter][3] * accepting_rate \
            - (runs - solutions[counter][3]) * (1 -accepting_rate)))
        if ctrl_max < ctrl_param:
            ctrl_max = ctrl_param
        counter += 1
    return ctrl_max


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
    ctrl_param = calculate_param(spark, solutions, data_df)

    solutions_schema = StructType([
        StructField("solution_id", IntegerType(), False),
        StructField("solution", ArrayType(DoubleType()), False),
        StructField("value", DoubleType(), False)
    ])
    to_continue = True
    checker = 0
    while to_continue and checker < max_steps:
        sa_results = optimize(spark, solutions, ctrl_param, data_df)
        new_solutions = sa_results[0]
        ctrl_param = sa_results[1]

        statistics = calculate_statistics(spark, new_solutions)
        dif_solutions = statistics[0]
        total_dif = statistics[1]
        value_max = statistics[2]
        value_min = statistics[3]
                
        if value_max - value_min < abs(percentage_error * value_min):
            to_continue = False
        
        next_generation = []
        # Generate a new list of solutions
        if checker >= min_steps:
            counter = 0
            solution_id = 0
            while solution_id < population:
                if counter < len(ceilings):
                    walker = 0
                    while walker < min(dif_solutions[counter][3]/total_dif, \
                        ceilings[counter]) * population:
                    
                        next_generation.append([solution_id, \
                            dif_solutions[counter][1], dif_solutions[counter][2]])
                        solution_id += 1
                        walker += 1
                else:
                    walker = 0
                    while walker < max(dif_solutions[counter][3]/total_dif * \
                        population, 1):
                        next_generation.append([solution_id, \
                            dif_solutions[counter][1], dif_solutions[counter][2]])
                        solution_id += 1
                        walker += 1
                counter += 1
        # On the initial runs, randomly select a set of solutions
        # so that the solutions spread evenly
        else:
            seed()
            solution_id = 0
            while solution_id < population:
                position = randint(0, len(dif_solutions)-1)
                next_generation.append([solution_id, dif_solutions[position][1], \
                    dif_solutions[position][2]])
                dif_solutions.pop(position)
                solution_id += 1
                
        solutions = next_generation           
        checker += 1
     
    solutions_df = spark.createDataFrame(solutions, solutions_schema)        
    solutions_df.write.mode("overwrite").parquet(result_path)
    print(checker)
    
if __name__ == "__main__":
    main()
        
    