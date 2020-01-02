from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import (DoubleType,
                               StructType,
                               StructField)
from random import randint
from random import uniform

def poly_evaluate(spark, solution, data_df, data_schema):
    """
    A function to evaluate a polynomial
    :param spark, an instance of SparkSession
    :param solution, a list or tuple (a0, a1, a2, ...), representing
        the parameters of y = a0 + a1*pow(x, 1) + a2*pow(x,2) + ...
        The order is sensative in calculating the value, so that first
        element of "solution" should be the parameter associating with
        power 0, second associating with power 1, and so on
    :param data_df, a spark DataFrame to be passed to calculate the
        value, representing the data needed for the calculation
    :param data_schema, a dictionary representing the schema, where
        data_schema["x_axis"] is the column name of x-axis and
        data_schema["y_axis"] is the column name of y-axis
    """

    counter = 0
    solution_schema = []
    while counter < len(solution):
        column_name = "a{}".format(counter)
        data_df = data_df.withColumn(column_name, \
            lit(solution[counter]))
        solution_schema.append(column_name)
        counter += 1
    
    data_df.createOrReplaceTempView("data")
    query = "SELECT sum(abs({} ".format(data_schema["y_axis"])
    counter = 0
    while counter < len(solution):
        query += "- {}*pow({}, {}) ".format( 
            solution_schema[counter], 
            data_schema["x_axis"], 
            counter)
        counter += 1
    query += ")) AS num FROM data"
    result_df = spark.sql(query)
    result = result_df.first()
    return result[0]

def poly_generate(solution):
    """
    Return a new list-typed solution based on the given solution
    :param solution, a list or tuple (a0, a1, a2, ...), representing
        the parameters of y = a0 + a1*pow(x, 1) + a2*pow(x,2) + ...
        The order is sensative in calculating the value, so that first
        element of "solution" should be the parameter associating with
        power 0, second associating with power 1, and so on
    """
    counter = 0
    delta = 0.01
        
    position = randint(0, len(solution)-1)
    direction = randint(0, 1)
    sign = randint(0, 1)
    mode = randint(0, 1)
    
    if sign == 0:
        multiplier = 1
    else:
        multiplier = -1
    
    new_solution = []
    counter = 0
    while counter < len(solution):
        if counter != position:
            new_solution.append(float(solution[counter]))
        else:
            if direction == 0:
                if mode == 0:
                    new_solution.append(float(multiplier * \
                        solution[counter] * (1+delta)))
                else:
                    new_solution.append(float(solution[counter] \
                        + delta))
            else:
                if mode == 0:
                    new_solution.append(float(multiplier * \
                        solution[counter] * (1-delta)))
                else:
                    new_solution.append(float(solution[counter] \
                        - delta))
        counter += 1
    return new_solution

def poly_initialize(size):
    """
    Generate and return a random list-typed solution based on "size"
    :param size, a integer representing the length of solution
    """
    
    solution = []
    for num in range(size):
        solution.append(float(uniform(-10.0, 10.0)))
    return solution
    
    