import geopandas as gp
import numpy as np
import pandas as pd
from shapely.geometry import Point
import dask_geopandas
from geopandas.tools import sjoin
from shapely.ops import unary_union



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
    points = np.dstack([grid_x.ravel(), grid_y.ravel()]).reshape(-1, 2)
    return [Point(x, y) for x, y in points]


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
        
    all_houses = gp.GeoDataFrame({'geometry': all_points}, geometry='geometry')
    # create spatial index
    sindex = shape_file.sindex
    
    # find the points that intersect with the polygons
    possible_matches_index = list(sindex.intersection(unary_union(all_houses['geometry']).bounds))
    possible_matches = shape_file.iloc[possible_matches_index]
    # Reproject crs of all houses
    all_houses.crs =possible_matches.crs

    precise_matches = sjoin(all_houses, possible_matches, how='inner', predicate='intersects')
    
    # create the mask before hand
    occupied = random_population(size=len(precise_matches), ratio=1 - empty_ratio)
    # calculate the sum once and use it later
    total_occupied = int(occupied.sum())
    race = random_population(size=total_occupied, ratio=1 - demographic_ratio)

    precise_matches['Race'] = None
    precise_matches.loc[occupied, 'Race'] = race.astype(int)
    return precise_matches[['Race', 'geometry']]




"""
def partition_data_by_hilbert(agent_houses_df, number_of_partitions):
    d_gdf = dask_geopandas.from_geopandas(
        agent_houses_df, npartitions=number_of_partitions
    )
    d_gdf = d_gdf.spatial_shuffle(by="hilbert")
    agent_houses_partitioned = gp.GeoDataFrame(
        columns=["Race", "geometry", "partition"]
    )
    for i in range(number_of_partitions):
        d_gdf_temp = d_gdf.partitions[i].compute().reset_index(drop=True)
        d_gdf_temp["partition"] = i + 1
        agent_houses_partitioned = pd.concat([agent_houses_partitioned, d_gdf_temp])
    return agent_houses_partitioned
"""




def split_arrays(array, number_of_partitions):
    if array.size==0:
        return [None]*(number_of_partitions+1)
    masks = [array[:, 3] == value for value in range(1, number_of_partitions + 1)]

    # Split the array based on the masks
    split_arrays = [array[mask][:, 0:3] for mask in masks]
    # Adding None because we don't need any data for the root node.
    return [None] + split_arrays


def move(unsatisfied_agents, empty_houses, number_of_partitions=4):
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

        (_, x_new, y_new, machine_key_eh) = concatenated_empty_house[
            np.random.choice(concatenated_empty_house.shape[0], 1),
            :,
        ][0]

        # remove from the empty houses because it is taken
        concatenated_empty_house = concatenated_empty_house[
            ~np.isclose(
                concatenated_empty_house[:, 1:4],
                np.array([x_new, y_new, machine_key_eh]),
            ).all(axis=1)
        ]
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
