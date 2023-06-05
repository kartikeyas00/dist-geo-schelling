import os
import itertools

partitions = [
    "row",
    "col",
    "hilbert",
    "morton",
    "geohash"
    ]

partition_combinations = list(itertools.product(partitions,repeat=2))

###############################################################################
## Various Pre Defined Settings
###############################################################################
python_file = "C:/Users/karti/Documents/College/University Of Oregon/Classes/CS 630 Distributed Systems/project/distributed-geographic-schellings-model/simulation/run_distributed.py"
shapefile = "shapefiles/CA/CA.shp"
spacing = 0.3
empty_ratio = 0.1
demography_ratio = 0.5
similarity_threshold = 0.3
number_of_processes = 8
number_of_iterations = 10
###############################################################################



for partition in partition_combinations:

    run_distributed_command = f"""
                mpiexec -n 8\
                python "{python_file}"\
                --shapefilepath "{shapefile}"\
                --spacing {spacing}\
                --empty_ratio {empty_ratio}\
                --demographic_ratio {demography_ratio} \
                --similarity_threshold {similarity_threshold}\
                --number_of_processes {number_of_processes} \
                --number_of_iterations {number_of_iterations} \
                --shape_file_partition "{partition[0]}" \
                --populated_houses_partition "{partition[1]}"
            """    

    command = f'''
                conda activate dist-geo-schelling & {run_distributed_command}
                '''
    print(command)            
    os.system(command)
    break