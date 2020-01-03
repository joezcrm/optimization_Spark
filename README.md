# optimization_Spark_EMR
## Purpose
This project tries to create a standardized optimization scripts for various optimization problems using Simulated
Annealing and Generic algorithms, Apache Spark, and Amazon EMR. As long as the components needed are provided, 
the optimization scripts included here should be independent of the actual problem.
## Python Scripts
### "/scripts/generate_data.py"
The script is used only to generate data for testing. It is not a necessary component for the project.
### "/scripts/sa_poly.py"
This is an example of a necessary component that defines functions to generate a new solution and to evaluate a solution.
The file defines how to generate a new (but random) set of parameters for a polinomial and how to evaluate the
polynomial.
### "/scripts/optimization.py"
This file defines the core ccomponents for the project. The script creates a set of random solutions based on the "sa_
poly" module and calculates the control parameter that will be needed later. Then, the script enters a reproduction-
selection loop, where the reproduction process is implemented by Simulated Annealing algorithm and the selection process
is implemented by a part of Generic Algorithm.
## Usage
A solution is evaluated based on a given set of data. In order to use the main part of the project, a user should modify 
"data_path", "data_schema", and "spark.read" to provide the data.  A user should provide the functions to generate
and evaluate solutions. To acieve the best performance, the user should also adjust the parameters as needed.
## Parameters
