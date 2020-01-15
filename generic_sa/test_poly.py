from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import (ArrayType,
                               DoubleType,
                               StructType,
                               StructField)
from random import random, randint, uniform, seed

def poly_generate(solution):
    """
    Return a new list-typed solution based on the given solution
    :param solution, a list or tuple (a0, a1, a2, ...), representing
        the parameters of y = a0 + a1*pow(x, 1) + a2*pow(x,2) + ...
        The order is sensative in calculating the value, so that first
        element of "solution" should be the parameter associating with
        power 0, second associating with power 1, and so on
    """
    seed()
    counter = 0
    delta = 0.1
        
    position = randint(0, len(solution)-1)
    direction = randint(0, 1)
    mode = randint(0, 1)

    new_solution = []
    counter = 0
    while counter < len(solution):
        if counter != position:
            new_solution.append(float(solution[counter]))
        else:
            if direction == 0:
                if mode == 0:
                    new_solution.append(float(\
                        solution[counter] * (1+delta)))
                else:
                    new_solution.append(float(solution[counter] \
                        + delta))
            else:
                if mode == 0:
                    new_solution.append(float(\
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
    seed()
    solution = []
    for num in range(size):
        solution.append(float(uniform(-10.0, 0)))
    return solution
    
@udf (DoubleType())
def evaluate_row(solution, x_axis, y_axis):
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

def evaluate_all(spark, solutions, data_df):
    """
    Return the resulting a list of solution_id, solution, and value
        result[][0] is solution_id;
        result[][1] is solution;
        result[][2] is value;
    :param solutions, a list;
        solutions[][0] is the integer typed solution_id;
        solutions[][1] is a list representing a current solution
    :param data_df, a spark DataFrame to be passed to calculate the
        value, representing the data needed for the calculation
    """ 
    evaluated_solutions = []
    counter = 0
    while counter < len(solutions):
        value = pow(solutions[counter][1][0], 6)/6 \
            - pow(solutions[counter][1][0], 5) * 16 /5 \
            + pow(solutions[counter][1][0], 4) * 95 / 4 \
            - pow(solutions[counter][1][0], 3) * 260 / 3 \
            + pow(solutions[counter][1][0], 2) * 162 \
            - solutions[counter][1][0] * 144
        evaluated_solutions.append([solutions[counter][0], \
            solutions[counter][1], value])
        counter += 1
        
    return evaluated_solutions
