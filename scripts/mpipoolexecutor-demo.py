import os
import time

from mpi4py.futures import MPIPoolExecutor


def worker(x):
    print(f"Running on process {os.environ['OMPI_COMM_WORLD_RANK']}")
    time.sleep(5.0)  # Simulate a time-consuming task
    return x * x


def callback(future):
    result = future.result()
    print(f"Result received on process {os.environ['OMPI_COMM_WORLD_RANK']}: {result}")


if __name__ == "__main__":
    with MPIPoolExecutor() as pool:
        futures = []
        for x in range(128):
            f = pool.submit(worker, x)
            f.add_done_callback(callback)
            futures.append(f)

        # Optionally, wait for all to complete
        # results = [f.result() for f in futures]
