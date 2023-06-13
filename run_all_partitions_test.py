import os
import itertools
from subprocess import Popen, PIPE

log_directory = "C:/Users/karti/Documents/College/University Of Oregon/Classes/CS 630 Distributed Systems/project/distributed-geographic-schellings-model/simulation/data/logs"#"/home/ksharma2/jobs/results/dist-geo-schelling/all_partitions/logs"
checkpoint_directory = "C:/Users/karti/Documents/College/University Of Oregon/Classes/CS 630 Distributed Systems/project/distributed-geographic-schellings-model/simulation/data/checkpoint"#"/home/ksharma2/jobs/results/dist-geo-schelling/all_partitions/checkpoint"
plot_directory = "C:/Users/karti/Documents/College/University Of Oregon/Classes/CS 630 Distributed Systems/project/distributed-geographic-schellings-model/simulation/data/plotting"#"/home/ksharma2/jobs/results/dist-geo-schelling/all_partitions/plotting"

partitions = [
    #"row",
    #"col",
    #"hilbert",
    #"morton",
    "geohash"
    ]

### Do some directory manipulation work

def clear_directory(directory_path):
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')
            
def create_directory(directory_path, delete=False):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    elif os.path.exists(directory_path) and delete:
        clear_directory(directory_path)
        
        
for partition in partitions:
    create_directory(f"{log_directory}/{partition}", delete=False)
    create_directory(f"{checkpoint_directory}/{partition}", delete=False)
    create_directory(f"{plot_directory}/{partition}", delete=False)
    for partition_ in partitions:
        create_directory(f"{log_directory}/{partition}/{partition_}", delete=True)
        create_directory(f"{checkpoint_directory}/{partition}/{partition_}", delete=True)
        create_directory(f"{plot_directory}/{partition}/{partition_}", delete=True)

partition_combinations = list(itertools.product(partitions,repeat=2))



###############################################################################
## Various Pre Defined Settings
###############################################################################
python_file = "C:/Users/karti/Documents/College/University Of Oregon/Classes/CS 630 Distributed Systems/project/distributed-geographic-schellings-model/simulation/run_distributed.py"
shapefile = "C:/Users/karti/Documents/College/University Of Oregon/Classes/CS 630 Distributed Systems/project/distributed-geographic-schellings-model/simulation/shapefiles/CA/CA.shp"#"/home/ksharma2/dist-geo-schelling/shapefiles/CA/CA.shp"
data_path = "C:/Users/karti/Documents/College/University Of Oregon/Classes/CS 630 Distributed Systems/project/distributed-geographic-schellings-model/simulation/data"#"/home/ksharma2/jobs/results/dist-geo-schelling/single/"
spacing = 0.1
empty_ratio = 0.1
demography_ratio = 0.71 # According to 2022 california census there are 71.1% of whites in the state
similarity_threshold = 0.4 # I just made it up
number_of_processes = 8
number_of_iterations = 100
python_path = "python"
###############################################################################



for partition in partition_combinations:

    command = f"""
                mpiexec -n {number_of_processes}\
                "{python_path}" "{python_file}"\
                --shapefilepath "{shapefile}"\
                --spacing {spacing}\
                --empty_ratio {empty_ratio}\
                --demographic_ratio {demography_ratio} \
                --similarity_threshold {similarity_threshold}\
                --number_of_processes {number_of_processes} \
                --number_of_iterations {number_of_iterations} \
                --shape_file_partition "{partition[0]}" \
                --populated_houses_partition "{partition[1]}"\
                --data_path "{data_path}"
            """    
    break
    process = Popen(command, shell=True,stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    
    print("Command output:")
    print(stdout.decode())
    
    if stderr:
        print("Command error:")
        print(stderr.decode())          
    