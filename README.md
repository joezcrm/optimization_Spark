# optimization_with_Spark
## Purpose
This project tries to create a standardized optimization scripts for various optimization problems using Simulated
Annealing algorithm and Apache Spark.
## Python Scripts
### "/scripts/generate_data.py"
The script is used only to generate data for testing.
### "/scripts/sa_poly.py"
The script defines how to generate solution initially and based on current solution.
### "/scripts/simulated_annealing.py"
The script is the main body for the project. The project first generates an initial random solution and calcultes
the first control parameter for the simulated annealing process. In the process, a temporary solution will be
generated and evluated. The process will check the value and decide whether to accept it as new solution or not.
## Usage
A solution is evaluated based on a given set of data. The data source, solution generation function, and solution evaluation
should be modified for real life application. The parameters should also be adjusted as well.
## Data
The data folder contains the data for testing and the solutions that _simulated_annealing.py_ generates. One can check that
the values for the solutions are decreasing.
