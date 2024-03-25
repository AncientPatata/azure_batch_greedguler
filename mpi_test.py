# Save this as mpi_sum.py
from mpi4py import MPI
import numpy as np
from log import *

if rank == 0:
    from b2sdk.v2 import *

    info = InMemoryAccountInfo()

    b2_api = B2Api(info)

    application_key_id = "005a863833255a00000000001"

    application_key = "K005KxufnOehfkMKAQZJnKftPeRkyrA"

    b2_api.authorize_account("production", application_key_id, application_key)
    bucket = b2_api.get_bucket_by_name("greedguler")

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
    
    bucket.upload_local_file(
        local_file="./mpi_output.txt",
        file_name="mpi_output.txt",
        file_infos={},
    )
    
    