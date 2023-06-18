import os
import itertools
from subprocess import Popen, PIPE
import yaml


###############################################################################
## Various Pre Defined Settings
###############################################################################
with open("/home/ksharma2/dist-geo-schelling/configs/config_dist_all_partitions.yml") as f:
    config = yaml.safe_load(f)
    python_file = config["python_file"]
    shapefile = config["shapefile"]
    data_path = config["data_path"]
    empty_ratio = config["empty_ratio"] 
    demography_ratio = config["demography_ratio"] # According to 2022 california census there are 71.1% of whites in the state
    similarity_threshold = config["similarity_threshold"] # I just made it up
    number_of_iterations = config["number_of_iterations"]
    python_path = config["python_path"] 
    number_of_processes = config["number_of_processes"]
    spacing = config["spacing"]
    timeout = config["timeout"]

###############################################################################

partitions = [
    "row",
    "col",
    "hilbert",
    "morton",
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
    create_directory(f"{data_path}/logs/{partition}", delete=False)
    create_directory(f"{data_path}/checkpoint/{partition}", delete=False)
    create_directory(f"{data_path}/plotting/{partition}", delete=False)
    for partition_ in partitions:
        create_directory(f"{data_path}/logs/{partition}/{partition_}/{spacing}", delete=True)
        create_directory(f"{data_path}/checkpoint/{partition}/{partition_}/{spacing}", delete=True)
        create_directory(f"{data_path}/plotting/{partition}/{partition_}/{spacing}", delete=True)

partition_combinations = list(itertools.product(partitions,repeat=2))


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
                --timeout {timeout}
            """    
    
    process = Popen(command, shell=True,stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    
    print("Command output:")
    print(stdout.decode())
    
    if stderr:
        print("Command error:")
        print(stderr.decode())          
    