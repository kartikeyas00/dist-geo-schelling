import os
import itertools
from subprocess import Popen, PIPE
import yaml


###############################################################################
## Various Pre Defined Settings
###############################################################################
with open("/home/ksharma2/dist-geo-schelling/configs/config_dist_partitions_with_spacing.yml") as f:
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
    #spacing = config["spacing"]
###############################################################################

partitions = [
    #["morton","row"], #done
    #["geohash","row"],#done
    #["geohash","geohash"],#done
    #["geohash","hilbert"],#done
    ["hilbert", "morton"],
    ["hilbert", "geohash"],
    ["geohash", "morton"],
    ["col", "row"],
    ["col", "geohash"],
    ["col", "morton"],
    ]

spacings = [
    0.1, 
    0.09, 
    0.08, 
    0.07, 
    0.06, 
    0.05,
    0.04, 
    0.03, 
    0.02, 
    0.01,
    0.009,
    0.008,
    0.007,
    0.006,
    0.005,
    0.004
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
    create_directory(f"{data_path}/logs/{partition[0]}", delete=False)
    create_directory(f"{data_path}/checkpoint/{partition[0]}", delete=False)
    create_directory(f"{data_path}/plotting/{partition[0]}", delete=False)
    create_directory(f"{data_path}/logs/{partition[0]}/{partition[1]}", delete=False)
    create_directory(f"{data_path}/checkpoint/{partition[0]}/{partition[1]}", delete=False)
    create_directory(f"{data_path}/plotting/{partition[0]}/{partition[1]}", delete=False)
    for spacing in spacings:
        create_directory(f"{data_path}/logs/{partition[0]}/{partition[1]}/{spacing}", delete=True)
        create_directory(f"{data_path}/checkpoint/{partition[0]}/{partition[1]}/{spacing}", delete=True)
        create_directory(f"{data_path}/plotting/{partition[0]}/{partition[1]}/{spacing}", delete=True)

combinations = list(itertools.product(partitions, spacings))
combinations = [list(partition) + [spacing] for partition, spacing in combinations]

for comb in combinations:

    command = f"""
                mpiexec -n {number_of_processes}\
                "{python_path}" "{python_file}"\
                --shapefilepath "{shapefile}"\
                --spacing {comb[2]}\
                --empty_ratio {empty_ratio}\
                --demographic_ratio {demography_ratio} \
                --similarity_threshold {similarity_threshold}\
                --number_of_processes {number_of_processes} \
                --number_of_iterations {number_of_iterations} \
                --shape_file_partition "{comb[0]}" \
                --populated_houses_partition "{comb[1]}"\
                --data_path "{data_path}"
            """    
    
    process = Popen(command, shell=True,stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    
    print("Command output:")
    print(stdout.decode())
    
    if stderr:
        print("Command error:")
        print(stderr.decode())          
    