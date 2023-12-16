import subprocess
import time
import matplotlib.pyplot as plt


def run_pipeline(workers, writers, chunksize, batchsize):
    args = [
        ".venv311\\Scripts\\python.exe",
        "src/pipeline.py",
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

    # Plotting the results
    workers, writers = zip(*[(config[0], config[1]) for config, _ in durations])
    times = [duration for _, duration in durations]

    plt.scatter(workers, times, label="Time per Configuration", color="r")
    plt.xlabel("Number of Workers (Writers = 12 - Workers)")
    plt.ylabel("Time (seconds)")
    plt.title("Performance with Different Worker/Writer Configurations")
    plt.legend()
    plt.show()


run_profiling()
