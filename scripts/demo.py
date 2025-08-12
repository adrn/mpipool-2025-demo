"""
This is an example script that uses mpi4py's "new" process pool to do a set of parallel calculations.
"""

import os
import time

from mpi4py.futures import MPIPoolExecutor


def worker(x):
    print(f"Running on process {os.environ['OMPI_COMM_WORLD_RANK']}")
    time.sleep(5.0)  # Simulate a time-consuming task
    return x * x


if __name__ == "__main__":
    with MPIPoolExecutor() as pool:
        results = list(pool.map(worker, range(128)))
