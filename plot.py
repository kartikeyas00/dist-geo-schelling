import json
from matplotlib import pyplot as plt
import numpy as np
file = open("history_gathered_all_houses.json", "rb")
simulation_data = json.load(file)

for sd in simulation_data:
    numpy_agent_houses_data = np.array(sd)
    plt.figure()
    plt.axis("off")
    plt.scatter(
        numpy_agent_houses_data[:, 1],
        numpy_agent_houses_data[:, 2],
        c=numpy_agent_houses_data[:, 0],
    )
    #for i in geometry:
        #plt.plot(i[:, 0], i[:, 1])
    plt.show()