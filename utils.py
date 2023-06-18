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
    all_houses = all_houses.drop_duplicates(["geometry"])
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
        np.hstack([empty_houses[i], np.full((empty_houses[i].shape[0], 1), i)])
        for i in range(len(empty_houses)) if empty_houses[i] is not None
    ],
    axis=0
    )

    concatenated_unsatisfied_agents = np.concatenate(
        [
            np.hstack([unsatisfied_agents[i], np.full((unsatisfied_agents[i].shape[0], 1), i)])
            for i in range(len(unsatisfied_agents)) if unsatisfied_agents[i] is not None
        ],
        axis=0
    )

    everything = np.concatenate((concatenated_unsatisfied_agents, concatenated_empty_house),axis=0)
    np.random.shuffle(everything[:, 0])

    
    
    satisfied_agents = everything[~np.isnan(everything[:,0])]
    concatenated_empty_house = everything[np.isnan(everything[:,0])]
    return split_arrays(satisfied_agents, number_of_partitions), split_arrays(concatenated_empty_house, number_of_partitions)
            


def move_centralized(unsatisfied_agents, empty_houses):
    satisfied_agents = []

    everything = np.concatenate((unsatisfied_agents, empty_houses),axis=0)
    np.random.shuffle(everything[:, 0])
    
    
    satisfied_agents = everything[~np.isnan(everything[:,0])]
    empty_houses = everything[np.isnan(everything[:,0])]

    return satisfied_agents, empty_houses