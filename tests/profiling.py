import csv
import itertools
import subprocess
import time
import matplotlib.pyplot as plt


def run_pipeline(workers, writers, chunksize, batchsize):
    args = [
        ".venv311\\Scripts\\python.exe",
        "src/main.py",
        "--chunksize",
        str(chunksize),
        "--workers",
        str(workers),
        "--writers",
        str(writers),
        "--batchsize",
        str(batchsize),
    ]
    start = time.time()
    subprocess.run(args)
    end = time.time()
    return end - start  # returns the duration of the run


def run_profiling():
    configurations = [(workers, 12 - workers, 1500, 1500) for workers in range(1, 12)]
    durations = []

    for config in configurations:
        duration = run_pipeline(*config)
        durations.append((config, duration))

    # Writing the results to a CSV file
    with open("profiling_results_workers.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Workers", "Writers", "Duration"])

        for config, duration in durations:
            workers, writers = config[0], config[1]
            writer.writerow([workers, writers, duration])


def run_profiling_cb():
    # Define ranges for chunksize and batchsize
    chunksizes = [100, 500, 1000, 5000]
    batchsizes = [100, 500, 1000, 5000]

    # Keep the optimal worker/writer ratio from previous profiling
    optimal_workers = 2  # Replace with your optimal values
    optimal_writers = 10  # Replace with your optimal values

    configurations = list(itertools.product(chunksizes, batchsizes))
    durations = []

    with open("profiling_results_chunksize.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Chunksize", "Batchsize", "Time"])

        for chunksize, batchsize in itertools.product(chunksizes, batchsizes):
            duration = run_pipeline(
                optimal_workers, optimal_writers, chunksize, batchsize
            )
            writer.writerow([chunksize, batchsize, duration])


run_profiling()
run_profiling_cb()
