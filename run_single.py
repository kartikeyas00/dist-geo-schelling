import logging
import time
from simulation import Simulation
from utils import load_shape_file, populate_simulation, move_centralized
import numpy as np
import pandas as pd
import json
import argparse


parser = argparse.ArgumentParser(description='Simulation settings from command line.')
parser.add_argument('--shapefilepath', default="shapefiles/CA/CA.shp", type=str, help='Path to the shape file.')
parser.add_argument('--spacing', default=0.1, type=float, help='Spacing value for the simulation.')
parser.add_argument('--empty_ratio', default=0.1, type=float, help='Empty ratio value for the simulation.')
parser.add_argument('--demographic_ratio', default=0.5, type=float, help='Demographic ratio value for the simulation.')
parser.add_argument('--similarity_threshold', default=0.3, type=float, help='Similarity threshold for the simulation.')
parser.add_argument('--number_of_iterations', default=10, type=int, help='Number of iterations for the simulation.')
parser.add_argument('--data_path', default="~", type=str, help='Path where the data needs to be stored.')

if __name__ == "__main__":
    start_time = time.time()  # Start time of the program

    args = parser.parse_args()
        

    ###########################################################################
    ### Simulation Settings
    ###########################################################################
    shapefilepath = args.shapefilepath
    spacing = args.spacing
    empty_ratio =args.empty_ratio
    demographic_ratio =args.demographic_ratio
    similarity_threshold = args.similarity_threshold
    number_of_iterations = args.number_of_iterations
    data_path = args.data_path
    log_path = f"{data_path}/logs/{spacing}"
    checkpoint_path = f"{data_path}/checkpoint/{spacing}"
    plotting_data_storage_path = f"{data_path}/plotting/{spacing}"
    ###########################################################################

    ###########################################################################
    
    logger = logging.getLogger('Process-Central')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f'{log_path}/Central.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info("Start time: %s seconds", start_time)
    logging.info("Central: Simulation settings loaded.")
    
    # Storage of important data for plotting and checkpointing purposes.
    history_all_houses = []


    # Data which needs to be scattered
    
    set_empty_houses = None
    set_satisfied_agents = None
    
    

    # Load shape file and partition data on parent node 0
    logging.info("Central: Loading shape file data.")
    shape_file = load_shape_file(shapefilepath)
    geometry = shape_file.geometry.apply(lambda x: np.array(x.exterior.coords[:-1]))

    # Scatter shape file partitions data to the workers(all nodes other than 0) from parent node 0

    start_time_agent_houses_population = time.time()
    agent_houses_populated = populate_simulation(
        shape_file, spacing, empty_ratio, demographic_ratio
    )
    end_time_agent_houses_population = time.time()
    total_time_agent_houses_population = end_time_agent_houses_population - start_time_agent_houses_population
    logger.info("Central: Populate Simulation.")
    logger.info(f"Central: Number of agents in Simulation ---> {len(agent_houses_populated[~pd.isna(agent_houses_populated.Race)])}")
    logger.info(f"Central: Total Time taken for populating agents ---> {total_time_agent_houses_population}")
    # Initialize the simulation on parent node 0
    
    sim = Simulation(
            agent_houses_populated, shapefilepath, spacing,
            similarity_threshold
        )
    logger.info("Central: Simulation Initialized.")
        
    for i in range(number_of_iterations):
        logger.info(f"Central: Starting iteration {i}")

        
        iteration_start_time = time.time()
            
        # Configure the simulation with empty houses and satisified agents
        sim.configure(set_empty_houses, set_satisfied_agents)
        logger.info("Central: Simulation configured.")
        sim.update()
        (
            empty_houses,
            unsatisfied_agents,
            houses,
        ) = sim.get_unsatisfied_and_empty_agents()
        logger.info("Central: Simulation Updated.")
                
        # Saving unsatisfied_agents, empty_houses, all_houses data for checkpointing
        # purposes
        
        
        checkpoint_data = {
                'iteration': i,
                'unsatisfied_agents':unsatisfied_agents.tolist(),
                'empty_houses':empty_houses.tolist(),
                'all_houses':houses.tolist(),
                }
        out_file = open(f"{checkpoint_path}/checkpoint.json", "w")
        json.dump(checkpoint_data, out_file)
            
        
        # Save plotting data, calculate new satisified agents and new empty houses
        # with the move function.
        
        set_satisfied_agents, set_empty_houses = move_centralized(
                unsatisfied_agents,
                empty_houses,
            )
        logger.info("Central: Agents moved.")
        if houses is not None:
            history_all_houses.append(houses.tolist())
         
        # Scatter satisfied and empty agents data to all workers from the parent node
       
        iteration_end_time = time.time()
        total_iteration_time = iteration_end_time - iteration_start_time
        logger.info(f"Central: Total Iteration time: {total_iteration_time} seconds.")
    
    # Save plotting data
    
    out_file = open(f"{plotting_data_storage_path}/history_gathered_all_houses.json", "w")
    json.dump(history_all_houses, out_file)
    
    end_time = time.time()  # End time of the program
    
    total_time = end_time - start_time
    
    logger.info("End time: %s seconds", end_time)
    logger.info("Total time: %s seconds", total_time)
