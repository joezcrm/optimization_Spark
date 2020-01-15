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
    # Calculate max value and minimum value
    value_max = solutions[0][2]
    value_min = solutions[0][2]
    for item in range(len(solutions)):
        if solutions[item][2] > value_max:
            value_max = solutions[item][2]
        if solutions[item][2] < value_min:
            value_min = solutions[item][2]

    # Calculate difference and reorder list
    dif_solutions = []
    total_dif = 0
    counter = 0
    solutions_length = len(solutions)
    while counter < solutions_length:
        dif = value_max - solutions[0][2]
        dif_position = 0
        dif_length = len(solutions)
        for item in range(dif_length):
            if dif < (value_max - solutions[item][2]):
                dif = value_max - solutions[item][2]
                dif_position = item
        dif_solutions.append([counter, solutions[dif_position][1],\
            solutions[dif_position][2], dif])
        total_dif += dif
        solutions.pop(dif_position)
        counter += 1
    return [dif_solutions, total_dif, value_max, value_min]