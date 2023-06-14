import geopandas as gp
import numpy as np
import pandas as pd
from shapely.geometry import Point
import dask_geopandas
from geopandas.tools import sjoin
from shapely.ops import unary_union
from shapely.prepared import prep



def load_shape_file(filename):
    return (
        gp.read_file(filename)
        .explode(index_parts=True)
        .reset_index()
        .drop(columns=["level_0", "level_1"])
    )



def generate_points(polygon, spacing):
    (minx, miny, maxx, maxy) = polygon.bounds
    x_coords = np.arange(np.floor(minx), np.ceil(maxx), spacing)
    y_coords = np.arange(np.floor(miny), np.ceil(maxy), spacing)
    grid_x, grid_y = np.meshgrid(x_coords, y_coords)
    points = np.column_stack([grid_x.ravel(), grid_y.ravel()])
    prepared_polygon = prep(polygon)
    mask = np.array([prepared_polygon.intersects(Point(x, y)) for x, y in points])
    points_within_polygon = [Point(x, y) for x, y in points[mask]]
    return points_within_polygon


def random_population(size, ratio):
    samples = np.zeros(size, dtype=np.bool_)
    samples[: round(size * ratio)] = 1
    np.random.shuffle(samples)
    return samples


def populate_simulation(
    shape_file: gp.GeoDataFrame,
    spacing: float,
    empty_ratio: float,
    demographic_ratio: float,
    races=2,
    random_seed=None,
):
    if random_seed is not None:
        np.random.seed(random_seed)
    all_points = []
    for polygon in shape_file.geometry:
        all_points.extend(generate_points(polygon, spacing))
    
    data = [{'geometry': point, 'Race': None} for point in all_points]

    all_houses = gp.GeoDataFrame(data, geometry='geometry')
    occupied = random_population(size=len(all_houses), ratio=1 - empty_ratio)
    # calculate the sum once and use it later
    total_occupied = int(occupied.sum())
    race = random_population(size=total_occupied, ratio=1 - demographic_ratio)
    all_houses.loc[occupied, 'Race'] = race.astype(int)
    return all_houses[['Race', 'geometry']]


def split_arrays(array, number_of_partitions):
    if array.size==0:
        return [None]*(number_of_partitions+1)
    masks = [array[:, 3] == value for value in range(1, number_of_partitions + 1)]

    # Split the array based on the masks
    split_arrays = [array[mask][:, 0:3] for mask in masks]
    # Adding None because we don't need any data for the root node.
    return [None] + split_arrays


def move_distributed(unsatisfied_agents, empty_houses, number_of_partitions=4):
    satisfied_agents = []

    concatenated_empty_house = np.concatenate(
        [
            np.append(
                empty_houses[i], np.full((empty_houses[i].shape[0], 1), i), axis=1
            )
            for i in range(len(empty_houses))
            if empty_houses[i] is not None
        ]
    )
    concatenated_unsatisfied_agents = np.concatenate(
        [
            np.append(
                unsatisfied_agents[i],
                np.full((unsatisfied_agents[i].shape[0], 1), i),
                axis=1,
            )
            for i in range(len(unsatisfied_agents))
            if unsatisfied_agents[i] is not None
        ]
    )
    print(
        f"Unsatisified Agents inside the move before ---> {len(concatenated_unsatisfied_agents)}"
    )
    print(f"Empty Houses inside the move before---> {len(concatenated_empty_house)}")

    for i in range(len(concatenated_unsatisfied_agents)):
        race, x, y, machine_key_ua = concatenated_unsatisfied_agents[i]
        random_index = np.random.choice(concatenated_empty_house.shape[0], 1)[0]
        """
        (_, x_new, y_new, machine_key_eh) = concatenated_empty_house[
            np.random.choice(concatenated_empty_house.shape[0], 1),
            :,
        ][0]
        """
        (_, x_new, y_new, machine_key_eh) = concatenated_empty_house[random_index]
        # remove from the empty houses because it is taken
        """
        concatenated_empty_house = concatenated_empty_house[
            ~np.isclose(
                concatenated_empty_house[:, 1:4],
                np.array([x_new, y_new, machine_key_eh]),
            ).all(axis=1)
        ]
        """
        concatenated_empty_house = np.delete(concatenated_empty_house,random_index,axis=0)
        # add to the empty house as the agent house is empty because the agent
        # has chosen a new house
        concatenated_empty_house = np.vstack(
            [concatenated_empty_house, [np.nan, x, y, machine_key_ua]]
        )

        satisfied_agents.append([race, x_new, y_new, machine_key_eh])
    satisfied_agents = np.array(satisfied_agents, dtype=float)
    print(f"Satisifed Agents inside the move after ---> {len(satisfied_agents)}")
    print(f"Empty Houses inside the move after---> {len(concatenated_empty_house)}")

    return split_arrays(satisfied_agents, number_of_partitions), split_arrays(concatenated_empty_house, number_of_partitions)
            


def move_centralized(unsatisfied_agents, empty_houses):
    satisfied_agents = []

    
    print(
        f"Unsatisified Agents inside the move before ---> {len(unsatisfied_agents)}"
    )
    print(f"Empty Houses inside the move before---> {len(empty_houses)}")

    for i in range(len(unsatisfied_agents)):
        race, x, y = unsatisfied_agents[i]
        random_index = np.random.choice(empty_houses.shape[0], 1)[0]
        (_, x_new, y_new) = empty_houses[
           random_index
        ]
        
        empty_houses = np.delete(empty_houses, random_index,axis=0)
        # add to the empty house as the agent house is empty because the agent
        # has chosen a new house
        empty_houses = np.vstack(
            [empty_houses, [np.nan, x, y]]
        )

        satisfied_agents.append([race, x_new, y_new,])
    satisfied_agents = np.array(satisfied_agents, dtype=float)
    print(f"Satisifed Agents inside the move after ---> {len(satisfied_agents)}")
    print(f"Empty Houses inside the move after---> {len(empty_houses)}")

    return satisfied_agents, empty_houses