import logging
import time
from mpi4py import MPI
from simulation import Simulation
from utils import load_shape_file, populate_simulation, move_distributed
from partition import partition_data
import numpy as np
import pandas as pd
import json
import argparse
import timeout_decorator

parser = argparse.ArgumentParser(description='Simulation settings from command line.')
parser.add_argument('--shapefilepath', default="shapefiles/CA/CA.shp", type=str, help='Path to the shape file.')
parser.add_argument('--spacing', default=0.1, type=float, help='Spacing value for the simulation.')
parser.add_argument('--empty_ratio', default=0.1, type=float, help='Empty ratio value for the simulation.')
parser.add_argument('--demographic_ratio', default=0.5, type=float, help='Demographic ratio value for the simulation.')
parser.add_argument('--similarity_threshold', default=0.3, type=float, help='Similarity threshold for the simulation.')
parser.add_argument('--number_of_processes', default=8, type=int, help='Number of processes for the simulation.')
parser.add_argument('--number_of_iterations', default=10, type=int, help='Number of iterations for the simulation.')
parser.add_argument('--shape_file_partition', default="hilbert", type=str, help='Type of shape file partition for the simulation.')
parser.add_argument('--populated_houses_partition', default="hilbert", type=str, help='Type of populated houses partition for the simulation.')
parser.add_argument('--data_path', default="~", type=str, help='Path where the data needs to be stored.')
parser.add_argument('--timeout', default="~", type=int, help='Timeout for crash failures or network failures detection.')

args = parser.parse_args()


###########################################################################
### Simulation Settings
###########################################################################
shapefilepath = args.shapefilepath
spacing = args.spacing
empty_ratio =args.empty_ratio
demographic_ratio =args.demographic_ratio
similarity_threshold = args.similarity_threshold
number_of_processes = args.number_of_processes
number_of_iterations = args.number_of_iterations
shape_file_partition = args.shape_file_partition
populated_houses_partition = args.populated_houses_partition
data_path = args.data_path
log_path = f"{data_path}/logs/{args.shape_file_partition}/{args.populated_houses_partition}/{number_of_processes}"
checkpoint_path = f"{data_path}/checkpoint/{args.shape_file_partition}/{args.populated_houses_partition}/{number_of_processes}"
plotting_data_storage_path = f"{data_path}/plotting/{args.shape_file_partition}/{args.populated_houses_partition}/{number_of_processes}"
timeout = args.timeout
###########################################################################

###########################################################################

@timeout_decorator.timeout(timeout)  # Timeout of 10 seconds
def worker_task(sim, set_empty_houses, set_satisfied_agents, rank, logger):
    sim.configure(set_empty_houses, set_satisfied_agents)
    logger.info(f"Rank {rank}: Simulation configured.")
    sim.update()
    (
        get_empty_houses,
        get_unsatisfied_agents,
        get_houses,
    ) = sim.get_unsatisfied_and_empty_agents()
    logger.info(f"Rank {rank}: Simulation Updated.")
    logger.info(f"Rank {rank}: Number of empty houses ---> {len(get_empty_houses)}.")
    logger.info(f"Rank {rank}: Number of unsatisfied agents ---> {len(get_unsatisfied_agents)}.")
    logger.info(f"Rank {rank}: Number of total houses ---> {len(get_houses)}.")
    return get_empty_houses, get_unsatisfied_agents, get_houses

if __name__ == "__main__":
    start_time = time.time()  # Start time of the program
    

    initialized = MPI.Is_initialized()
    
    
    
    # Initialize MPI
    if not initialized:
        MPI.Init()
        

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    
    logger = logging.getLogger(f'Process-{rank}')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f'{log_path}/app-{rank}.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info("Rank: %s", rank)
    logger.info("Start time: %s seconds", start_time)
    logger.info("MPI Initialized: %s", initialized)
    logging.info(f"Rank {rank}: Simulation settings loaded.")
    
    # Storage of important data for plotting and checkpointing purposes.
    history_gathered_all_houses = []
    
    # Data which needs to be gathered
    get_agent_houses_populated = None
    get_unsatisfied_agents = None
    get_empty_houses = None
    get_houses = None

    # Data which needs to be scattered
    set_new_satisifed_agents = None
    set_new_empty_houses = None
    set_empty_houses = None
    set_satisfied_agents = None
    set_shape_file_partition = None
    set_agent_houses_populated_partition = None
    

    # Load shape file and partition data on parent node 0
    start_time_agent_houses_population = time.time()
    if rank == 0:
        logging.info(f"Rank {rank}: Loading shape file and partitioning data.")
        shape_file = load_shape_file(shapefilepath)
        shape_file_partition = partition_data(
            shape_file, 
            number_of_partitions=number_of_processes - 1,
            kind=shape_file_partition,
            for_="shape"
        )
        set_shape_file_partition = [
            None
            if i == 0
            else shape_file_partition[shape_file_partition["partition"] == i]
            for i in range(number_of_processes)
        ]
        geometry = shape_file.geometry.apply(lambda x: np.array(x.exterior.coords[:-1]))
        
    
    # Scatter shape file partitions data to the workers(all nodes other than 0) from parent node 0
    shape_file_partition_scattered = comm.scatter(set_shape_file_partition, root=0)
    logger.info(f"Rank {rank}: Shape file partition scattered.")
    
    if rank != 0:
        get_agent_houses_populated = populate_simulation(
            shape_file_partition_scattered, spacing, empty_ratio, demographic_ratio
        )
        
    #Gather all populated partitions on parent node 0
    agent_houses_populated_gathered = comm.gather(get_agent_houses_populated, root=0)
    end_time_agent_houses_population = time.time()
    total_time_agent_houses_population = end_time_agent_houses_population - start_time_agent_houses_population
    logger.info(f"Rank {rank}: Total Time taken for populating agents ---> {total_time_agent_houses_population}")
    logger.info(f"Rank {rank}: Shape file partition gathered.")
    
    # Partition populated agent data on parent node 0
    if rank ==0 :
        logger.info(f"Rank {rank}: Partitioning agent house populated data.")
        
        agent_houses_populated_gathered = pd.concat(
            [i for i in agent_houses_populated_gathered if i is not None])
        logger.info(f"Rank {rank}: Number of agents in Simulation ---> {len(agent_houses_populated_gathered[~pd.isna(agent_houses_populated_gathered.Race)])}")
    
        agent_houses_populated_partition = partition_data(
            agent_houses_populated_gathered, 
            number_of_partitions=number_of_processes - 1,
            kind=populated_houses_partition,
            for_="agents"
        )
        set_agent_houses_populated_partition = [
            None
            if i == 0
            else agent_houses_populated_partition[agent_houses_populated_partition["partition"] == i]
            for i in range(number_of_processes)
        ]
    # Scatter populated agents partition to the workers(all nodes other than 0) from parent node 0
    agent_houses_populated_partition_scattered = comm.scatter(set_agent_houses_populated_partition, root=0)
    logger.info(f"Rank {rank}: Agent house populated data scattered.")
    # Initialize the simulation on parent node 0
    if rank != 0:
        sim = Simulation(
            agent_houses_populated_partition_scattered, shapefilepath, spacing,
            similarity_threshold
        )
        logger.info(f"Rank {rank}: Simulation Initialized.")
        
    for i in range(number_of_iterations):
        logger.info(f"Rank {rank}: Starting iteration {i}")
        iteration_start_time = time.time()
        if rank != 0:
            try:
                # Configure the simulation with empty houses and satisified agents
                get_empty_houses, get_unsatisfied_agents, get_houses = worker_task(sim, set_empty_houses, set_satisfied_agents, rank, logger)
                iteration_end_time_others = time.time()
                total_iteration_time = iteration_end_time_others - iteration_start_time
                logger.info(f"Rank {rank}: Total Iteration time: {total_iteration_time} seconds.")
            except timeout_decorator.timeout_decorator.TimeoutError:
               logger.error(f"Rank {rank}: Worker task exceeded the time limit.")
               break
        # Gathered data from the workers on the parent node 0
        gathered_unsatisfied_agents = comm.gather(get_unsatisfied_agents, root=0)
        gathered_empty_houses = comm.gather(get_empty_houses, root=0)
        gathered_all_houses = comm.gather(get_houses, root=0)
        logger.info(f"Rank {rank}: Unsatisfied agents, empty houses and all houses gathered.")
        
        # Saving unsatisfied_agents, empty_houses, all_houses data for checkpointing
        # purposes
        

        if rank == 0:

            checkpoint_data = {
                'iteration': i,
                'unsatisfied_agents': np.concatenate(
                    [arr for arr in gathered_unsatisfied_agents if arr is not None]
                ).tolist(),
                'empty_houses': np.concatenate(
                    [arr for arr in gathered_empty_houses if arr is not None]
                ).tolist(),
                'all_houses': np.concatenate(
                    [arr for arr in gathered_all_houses if arr is not None]
                ).tolist(),
                }
            out_file = open(f"{checkpoint_path}/checkpoint.json", "w")
            json.dump(checkpoint_data, out_file)
            
        
        # Save plotting data, calculate new satisfied agents and new empty houses
        # with the move function.
        if rank == 0:
            gathered_all_houses = np.concatenate(
                [arr for arr in gathered_all_houses if arr is not None]
            )
            move_start_time = time.time()
            set_new_satisfied_agents, set_new_empty_houses = move_distributed(
                gathered_unsatisfied_agents,
                gathered_empty_houses,
                number_of_processes - 1,
            )
            move_end_time = time.time()
            total_move_time = move_end_time - move_start_time
            logger.info(f"Rank {rank}: Agents moved.")
            logger.info(f"Rank {rank}: Total Move time: {total_move_time} seconds.")
            if gathered_all_houses is not None:
                history_gathered_all_houses.append(gathered_all_houses.tolist() )
         
        # Scatter satisfied and empty agents data to all workers from the parent node
        set_satisfied_agents = comm.scatter(set_new_satisfied_agents, root=0)
        set_empty_houses = comm.scatter(set_new_empty_houses, root=0)
        logger.info(f"Rank {rank}: Unsatisfied agents and empty houses scattered.")
        if rank ==0:
            iteration_end_time_0 = time.time()
            total_iteration_time = iteration_end_time_0 - iteration_start_time
            logger.info(f"Rank {rank}: Total Iteration time: {total_iteration_time} seconds.")
    
    # Save plotting data
    if rank == 0:
        out_file = open(f"{plotting_data_storage_path}/history_gathered_all_houses.json", "w")
        json.dump(history_gathered_all_houses, out_file)
    if initialized:
        MPI.Finalize()
    
    end_time = time.time()  # End time of the program
    
    total_time = end_time - start_time
    
    logger.info("End time: %s seconds", end_time)
    logger.info("Total time: %s seconds", total_time)
