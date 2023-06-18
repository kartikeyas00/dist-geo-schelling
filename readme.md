# Distributed Geo-spatial Schelling’s Segregation Model

Schelling’s model of segregation is an important agent-based model in social sciences. However, its real-world geographical application has been limited due to computational constraints.To overcome this limitation, we introduce a novel
distributed geo-spatial version of Schelling’s model. This research utilizes various geo-spatial partitioning techniques to evenly balance partitions and optimize computational efficiency. Experimental results indicates that Morton and Geohash partitioning methods effectively distribute workload and provide consistent performance.Furthermore, our distributed system significantly outperform its centralized counterpart, achieving a speedup of up to 17 times for simulations involving millions of agents. Importantly, our system show excellent horizontal scalability, delivering linear speedup as the number of processes increased.

![Geo-spatial Schelling’s Segregation Model Simulation Gif](images/visualization.gif)

## How to Run?

First you need to make sure that you have a working MPI implementation, preferably supporting MPI-3. Information about building MPI from source could 
be found [here](https://mpi4py.readthedocs.io/en/stable/appendix.html#building-mpi)

First make sure you have Anaconda distribution installed. It can be installed from [here](https://www.anaconda.com/download)

Then ```cd``` into the directory where you have downloaded the repository and then run the following command to create a conda environment that would contain all the python packages needed for this system.

```
conda env create -f environment.yml
```

To run the distributed version, you can use the commands below:
```bash
$ conda activate dist-geo-schelling

$ mpiexec -n {number_of_processes}\
    <python_path> <python_run_file>\
    --shapefilepath <shapefile>\
    --spacing <spacing>\
    --empty_ratio <empty_ratio>\
    --demographic_ratio <demography_ratio> \
    --similarity_threshold <similarity_threshold>\
    --number_of_processes <number_of_processes> \
    --number_of_iterations <number_of_iterations> \
    --shape_file_partition <shape_file_partition> \
    --populated_houses_partition <populated_houses_partition>\
    --data_path <path to store simulation data>\
    --timeout <timeout>
```

To run the single version, you can use the commands below:

```bash
$ conda activate dist-geo-schelling

$ <python_path> <python_run_file>\
    --shapefilepath <shapefile>\
    --spacing <spacing>\
    --empty_ratio <empty_ratio>\
    --demographic_ratio <demography_ratio> \
    --similarity_threshold <similarity_threshold>\
    --number_of_iterations <number_of_iterations> \
    --data_path <path to store simulation data>\
```