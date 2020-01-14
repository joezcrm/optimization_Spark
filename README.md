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
The solutions are selected and reproduced based on their values.
## Usage
A solution is evaluated based on a given set of data. The data source, solution generation function, and solution evaluation
should be modified for real life application. All the solution generation function and solution evluation function
should return the same data types as those in **/generic_sa/poly.py**. After defining these functions in a new module,
the **poly** module name in **/generic_sa/generic_sa.py** should be replaced by the new module name.
