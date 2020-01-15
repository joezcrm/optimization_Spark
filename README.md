# optimization_with_Spark
## Purpose
This project tries to create a standardized optimization scripts for various optimization problems using generic
algorithm, Simulated Annealing algorithm, and Apache Spark.
## Python Scripts
### "/generic_sa/generate_data.py"
The script is used only to generate data for testing.
### "/generic_sa/poly.py"
The script defines how to generate solution initially and based on current solution. This script also defines how to
evaluate a solution based on a set of data.
### "/generic_sa/test_poly.py"
The script is used for testing only.  It is the same as the poly.py except that a solution is evaluated with a given
polynomial.
### "/generic_sa/generic_sa.py"
The script is the main body for the project. The project generates an initial random solution and calcultes 
the initial control parameter for the simulated annealing process. In the process, a temporary solution will be
generated and evluated. The process will check the value and decide whether to accept it as new solution or not.
In the initial runs before _min_steps_, the solutions generated are to be selected randomly to allow the solutions 
to spread evenly across the potential region. After _min_steps_, the solutions are selected and reproduced based on
their values.
## Usage
A solution is evaluated based on a given set of data. The data source, solution generation function, and solution evaluation
should be modified for real life application. All the solution generation function and solution evluation function
should return the same data types as those in **/generic_sa/poly.py**. After defining these functions in a new module,
the **poly** module name in **/generic_sa/generic_sa.py** should be replaced by the new module name. For testing purpose,
the script **generic_sa.py** use python directly to calculate statistics directly. In a real life application, since the
generated solutions will be large, the **statistics_python** module should be replaced by **statistics_spark**.
## Test Result
For testing purpose, **generic_sa.py** sets the argument _solution_length_ to be 1 and uses **test_poly** module to calculate 
the minimum value of the given polinomial, which has local minimum at x = 1 and x = 3 and global minimum at x = 6.
The **test_poly** initializes solutions at a value ranged from -10 to 0 away from x = 6 and the results match the global minimum.
