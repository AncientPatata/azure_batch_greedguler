# Save this as mpi_sum.py
from mpi4py import MPI
import requests

def create_logging_function(machine_id):
    try:
        with open('./ngrok_url.txt', 'r') as url_file:
            ngrok_url = url_file.read().strip()
        log_endpoint = f"{ngrok_url}/log"
        def logger(msg):
            requests.post(log_endpoint, json={"machine": machine_id,"message":msg})
        return logger
    except Exception as e:
        print(f"Error sending log message: {e}")
        return print


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

if rank == 0:
    from b2sdk.v2 import *

    info = InMemoryAccountInfo()

    b2_api = B2Api(info)

    application_key_id = "005a863833255a00000000001"

    application_key = "K005KxufnOehfkMKAQZJnKftPeRkyrA"

    b2_api.authorize_account("production", application_key_id, application_key)
    bucket = b2_api.get_bucket_by_name("greedguler")

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
    
    