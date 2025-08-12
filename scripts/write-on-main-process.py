"""
This is an example script that demonstrates how to use a paradigm that Adrian uses a lot
when bulk processing data or generating a lot of data in parallel runs. I have found
that the mpi-enabled hdf5 functionality for parallel I/O to be hard to work with, so
this is a workaround for that workflow.

Here is a concrete example of a case where this has been useful: I want to compute
action-angle coordinates for every star in a catalog of data from the Gaia mission. This
involves looping row by row through the catalog, performing some computation, and then
writing the outputs back to a different catalog. The catalog has many rows, and the
calculation is fairly fast per row, so I often "batch" up the rows to send off to a
worker.

When the worker finishes, it has action-angle coordinates for a batch of rows from the
Gaia catalog, and I need to write those back to the appropriate place in an output
catalog. Multiple processes in general cannot / should not try to write to the same file
because you can end up corrupting the file if the I/O library does not support parallel
write operations. There are a few ways you can work around this:
- Option 1: Keep everything in memory, merge them when all rows are done, and then write
  the full file all at once from a single process at the end. This is the simplest
  option, but can require a lot of memory (depending on how much output data is
  generated) and can waste calculation if your script fails or has to be killed mid-run.
- Option 2: Write each row or batch of rows to a separate file. This is straightforward
  to implement, but it does not scale well -- it is generally a bad idea to write
  thousands or more files to a filesystem, and you then have to think about how many
  files you have per directory, how fast you are writing the files, etc.
- Option 3: Return the outputs to the main process and have it handle the writing.
  This can be more efficient because you can batch writes together, but it requires
  more coordination between the worker processes and the main process and can lead to
  some slight performance hits if worker processes have to wait for the main process to
  finish other write operations before they can hand off their results. This option is
  safer in terms of avoiding file corruption.
"""

import itertools
import os
import pathlib
import time

import h5py
import numpy as np
from mpi4py.futures import MPIPoolExecutor

this_path = pathlib.Path(__file__).parent


def worker(idxs, data, filename):
    print(f"Running on process {os.environ['OMPI_COMM_WORLD_RANK']}")
    time.sleep(5.0)  # Simulate a time-consuming task
    return {"idxs": idxs, "data": {"z": data["x"] * data["y"]}, "filename": filename}


def callback(future):
    result = future.result()
    print(
        f"Result received on process {os.environ['OMPI_COMM_WORLD_RANK']}: "
        f"{result['data']}"
    )

    with h5py.File(result["filename"], "r+") as f:
        f["z"][result["idxs"]] = result["data"]["z"]


if __name__ == "__main__":
    N_data = 1024
    N_batches = 32

    # Make sure file exists:
    output_filename = this_path / "test-output.hdf5"
    with h5py.File(output_filename, "w") as f:
        f.create_dataset("z", data=np.full(N_data, np.nan, dtype=np.float64))

        # Verify that all values are nan to start with:
        assert np.isnan(f["z"]).all()

    data = np.linspace(0, 1, N_data)
    batches = [np.array(x) for x in itertools.batched(np.arange(N_data), N_batches)]

    with MPIPoolExecutor() as pool:
        futures = []
        for batch_idx in batches:
            f = pool.submit(
                worker,
                batch_idx,
                data={"x": data[batch_idx], "y": data[batch_idx] + 4},
                filename=output_filename,
            )
            f.add_done_callback(callback)
            futures.append(f)

    # After processing, no values should be nan:
    with h5py.File(output_filename, "r") as f:
        assert np.isfinite(f["z"]).all()
