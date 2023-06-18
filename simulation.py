from utils import load_shape_file
import numpy as np
from scipy.spatial import cKDTree
from collections import defaultdict


class Simulation:
    def __init__(self, houses, shapefilepath, spacing, similarity_threshold):
        self.initial_houses = houses
        self.houses = None
        self.shapefilepath = shapefilepath
        self.similarity_threshold = similarity_threshold
        self.spacing = spacing
        self.geometry = None
        self.unsatisfied_agents = None
        self.unsatisfied_agents_index = []

    def configure(self, empty_houses=None, satisfied_agents=None):
        self.geometry = list(
            load_shape_file(self.shapefilepath).geometry.apply(
                lambda x: np.array(x.exterior.coords[:-1])
            )
        )

        if self.houses is None:
            self.houses = np.array(
                [
                    self.initial_houses.Race,
                    self.initial_houses.geometry.x,
                    self.initial_houses.geometry.y,
                ],
                dtype=float,
            ).T
        if empty_houses is not None:
            self.houses = np.vstack([self.houses, empty_houses])

        if satisfied_agents is not None:
            self.houses = np.vstack([self.houses, satisfied_agents])

    def is_unsatisfied(self, agent, all_neighbours, race_list):
        neighbours_indices = all_neighbours[agent]
        race = race_list[agent]
        if np.isnan(race):
            return False
        neighbours = race_list[neighbours_indices]
        if len(neighbours) == 0:
            return False
        else:
            return (
                len(neighbours[neighbours == race]) / len(neighbours)
                < self.similarity_threshold
            )

    def populate_unsatisfied_agents(
        self, index, all_neighbours, race_list, agent_houses
    ):
        if self.is_unsatisfied(index, all_neighbours, race_list):
            self.unsatisfied_agents_index.append(index)
            self.unsatisfied_agents = np.append(
                self.unsatisfied_agents,
                [[race_list[index], agent_houses[index][0], agent_houses[index][1]]],
                axis=0,
            )

            return 1
        else:
            return 0

    def update(self):
        old_houses = self.houses[:, [1, 2]].copy()
        race = self.houses[:, 0].copy()
        all_neighbours = defaultdict(list)
        tree = cKDTree(old_houses)
        for i, j in tree.query_pairs(self.spacing * 2):
            all_neighbours[i].append(j)
            all_neighbours[j].append(i)
        changes = 0
        self.unsatisfied_agents = np.empty((0, 3))
        self.unsatisfied_agents_index = []
        for index in np.arange(len(old_houses)):
            changes += self.populate_unsatisfied_agents(
                index,
                all_neighbours,
                race,
                old_houses,
            )

    def get_unsatisfied_and_empty_agents(self):
        empty_houses = self.houses[np.isnan(self.houses[:, 0])]
        old_houses = self.houses.copy()
    
        self.houses = np.delete(self.houses,self.unsatisfied_agents_index,axis=0)
        self.houses = self.houses[~np.isnan(self.houses[:, 0])]
       
        return empty_houses, self.unsatisfied_agents, old_houses
