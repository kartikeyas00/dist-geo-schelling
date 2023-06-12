import os
from subprocess import Popen, PIPE

log_directory = "/home/ksharma2/jobs/results/dist-geo-schelling/single/logs"
checkpoint_directory = "/home/ksharma2/jobs/results/dist-geo-schelling/single/checkpoint"
plot_directory = "/home/ksharma2/jobs/results/dist-geo-schelling/single/plotting"

spacings = [
    #0.1, 
    #0.09, 
    #0.08, 
    #0.07, 
    #0.06, 
    #0.05,
    #0.04, 
    #0.03, 
    #0.02, 
    0.01
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
        
for spacing in spacings:
    create_directory(f"{log_directory}/{spacing}", delete=True)
    create_directory(f"{checkpoint_directory}/{spacing}", delete=True)
    create_directory(f"{plot_directory}/{spacing}", delete=True)




###############################################################################
## Various Pre Defined Settings
###############################################################################
python_file = "/home/ksharma2/dist-geo-schelling/run_single.py"
shapefile = "/home/ksharma2/dist-geo-schelling/shapefiles/CA/CA.shp"
data_path = "/home/ksharma2/jobs/results/dist-geo-schelling/single/"
#spacing = 0.1
empty_ratio = 0.1
demography_ratio = 0.71 # According to 2022 california census there are 71.1% of whites in the state
similarity_threshold = 0.4 # I just made it up
number_of_iterations = 100
python_path = "python"
###############################################################################


for spacing in spacings:

    command = f"""
                "{python_path}" "{python_file}"\
                --shapefilepath "{shapefile}"\
                --spacing {spacing}\
                --empty_ratio {empty_ratio}\
                --demographic_ratio {demography_ratio} \
                --similarity_threshold {similarity_threshold}\
                --number_of_iterations {number_of_iterations} \
                --data_path "{data_path}"
            """    


    process = Popen(command, shell=True,stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    print("Command output:")
    print(stdout.decode())

    if stderr:
        print("Command error:")
        print(stderr.decode())          
