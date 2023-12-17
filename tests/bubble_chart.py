import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Reading the data from a CSV file
df = pd.read_csv("profiling_results_chunksize.csv")

# Normalize the 'Time' data
time_min = df["Time"].min()
time_max = df["Time"].max()
df["NormalizedTime"] = (df["Time"] - time_min) / (time_max - time_min)

# Apply a scaling factor
scaling_factor = 1000  # Adjust this factor as needed
df["ScaledTime"] = df["NormalizedTime"] * scaling_factor


print(df)

# Plotting the bubble chart
plt.scatter("Chunksize", "Batchsize", s="ScaledTime", alpha=0.5, data=df)

plt.xlabel("Chunksize")
plt.ylabel("Batchsize")
plt.title("Bubble Chart of Chunksize, Batchsize and Time")
plt.show()
