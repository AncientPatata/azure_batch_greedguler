# Save this as mpi_sum.py
from mpi4py import MPI
import numpy as np
from log import *


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

logger = create_logging_function(rank)

logger("Test")
# Each process will have a local_num
local_num = rank + 1

# Use MPI to compute the sum of local_nums across all processes
total_sum = comm.reduce(local_num, op=MPI.SUM, root=0)

# Only the root process will print the result
if rank == 0:
    print(f"Total sum is {total_sum}")
    logger(f"Total sum is {total_sum}")

    # Save the result to a file
    with open('mpi_output.txt', 'w') as f:
        f.write(f"Total sum is {total_sum}")