from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import (ArrayType,
                               DoubleType,
                               StructType,
                               StructField)
from random import randint, uniform, seed

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
    delta = 0.01
        
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
        solution.append(float(uniform(-10.0, 10.0)))
    return solution
    
    