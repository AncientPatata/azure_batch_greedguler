# Save this as mpi_sum.py
from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# Each process will have a local_num
local_num = rank + 1

# Use MPI to compute the sum of local_nums across all processes
total_sum = comm.reduce(local_num, op=MPI.SUM, root=0)

# Only the root process will print the result
if rank == 0:
    print(f"Total sum is {total_sum}")
    # Save the result to a file
    with open('mpi_output.txt', 'w') as f:
        f.write(f"Total sum is {total_sum}")