from mpi4py import MPI
import numpy as np
import json 
import rustworkx as rx
import timeit
from datetime import datetime, timedelta
import requests
from pathlib import Path
import os
import gdown

def create_logging_function(machine_id):
    try:
        # with open('./ngrok_url.txt', 'r') as url_file:
        #     ngrok_url = url_file.read().strip()
        log_endpoint = "https://1b5b-138-195-40-100.ngrok-free.app/log"
        def logger(msg):
            print(f"machine {machine_id} =>" + str(msg))
            requests.post(log_endpoint, json={"machine": machine_id,"message":msg})
        return logger
    except Exception as e:
        print(f"Error sending log message: {e}")
        return print



comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def load_dag_from_json_rx(filepath):
    logger("Loading DAG from JSON file " + filepath + "....")  # TODO: Custom logging with control of verbosity.
    start_time = timeit.default_timer()
    graph = rx.PyDiGraph()
    durations = {}
    nodes_list = []
    edges_list = []
    with open(filepath, "r") as file_handle:
        object_data = json.load(file_handle)
        nodes = object_data["nodes"]
        for node_id, node_data in nodes.items():
            time_parts = node_data["Data"].split(':')
            duration = timedelta(hours=int(time_parts[0]), minutes=int(time_parts[1]), seconds=float(time_parts[2]))
            durations[int(node_id)] = duration  # Storing duration in the durations dictionary
            nodes_list.append(int(node_id))
            edges_list += [(dep, int(node_id)) for dep in node_data["Dependencies"]]
    del object_data
    node_indices = graph.add_nodes_from(nodes_list)
    mapping = dict(zip(nodes_list, node_indices))
    del nodes_list
    del node_indices
    new_edges_list = [(mapping[a], mapping[b]) for a, b in edges_list]
    del edges_list
    graph.add_edges_from_no_data(new_edges_list)
    del new_edges_list
    elapsed = timeit.default_timer() - start_time
    print("Loading file took:", elapsed)  # TODO: timeit for JSON file loading

    return graph, durations

def leveled_topological_sort(graph: rx.PyDiGraph):
    graph = graph.copy()
    new_queue = [n for n in graph.node_indices() if graph.in_degree(n) == 0]
    node_queue = [new_queue]
    while len(new_queue) > 0:
        successors = []
        out_edges = []
        for q in new_queue:
            for n in graph.successor_indices(q):
                out_edges.append((q,n))
                successors.append(n)
        successors = set(successors)
        
        graph.remove_edges_from(out_edges)
        graph.remove_nodes_from(new_queue)
        node_queue.append(new_queue)
        new_queue = [n for n in successors if graph.in_degree(n) == 0]
    return node_queue

def earliest_start_time_optimized(task, graph, schedule):
    # Calculate earliest start time for a task on a machine respecting dependencies
    dependencies = list(graph.predecessors(task))
    # print("task: ", task)
    # print("depend: ", dependencies)
    # print("schedule: ", schedule)
    if not dependencies:
        return 0
    else:
        max_end_time = max(schedule.get(job_index, {"end_time": 0})["end_time"] for job_index in dependencies )
        return max_end_time

def allocate_jobs_to_machines_with_heuristic_rx(graph: (rx.PyDiGraph, dict),queue_list, num_machines=8):
    man_graph = graph[0]
    durations = graph[1]
    jobs = {}
    

    free_time = [0] * num_machines

    
    for queue in queue_list:
        for job in queue:
            job_index = man_graph.get_node_data(job)
            # print("Job : ", job)
            machine = min(range(num_machines), key=lambda machine: free_time[machine])
            duration = durations[job_index]
            earliest_start_time_for_job = earliest_start_time_optimized(job, graph[0],jobs) #todo
            # do machine choice after (by also taking into account how far back we can go)
            start_time = max([free_time[machine], earliest_start_time_for_job])
            end_time = start_time + duration.total_seconds()
            jobs[job_index] = {'start_time': start_time, 'end_time': end_time,
                                                                'duration': end_time - start_time, 'machine_index': machine}
            free_time[machine] = end_time

    return jobs
        
def split_list_into_sublists_with_remainder(lst, n):
    chunk_size = len(lst) // n
    result = []
    for i in range(n-1):
        result.append(lst[chunk_size*i: chunk_size* (i+1)])
    result.append(lst[chunk_size*(n-1):])
    return result


## Upload results to backblaze storage bucket (Azure storage permission problem..)
if rank == 0:
    from b2sdk.v2 import *

    info = InMemoryAccountInfo()

    b2_api = B2Api(info)

    application_key_id = "005a863833255a00000000001"

    application_key = "K005KxufnOehfkMKAQZJnKftPeRkyrA"

    b2_api.authorize_account("production", application_key_id, application_key)
    bucket = b2_api.get_bucket_by_name("greedguler")
    perflogs = []


logger = create_logging_function(rank)

runs = [
    # {"path_in_str": "smallComplex.json", "num_machines": 4 },
    # {"path_in_str": "smallComplex.json", "num_machines": 6 },
    {"path_in_str": "smallComplex.json", "num_machines": 8 },
    # {"path_in_str": "smallRandom.json", "num_machines": 4 },
    # {"path_in_str": "smallRandom.json", "num_machines": 6 },
    {"path_in_str": "smallRandom.json", "num_machines": 8 },
    {"path_in_str": "xsmallComplex.json", "num_machines": 4 },
    # {"path_in_str": "xsmallComplex.json", "num_machines": 2 },
    {"path_in_str": "xsmallComplex.json", "num_machines": 6 },
    # {"path_in_str": "xsmallComplex.json", "num_machines": 8 },
    # {"path_in_str": "MediumComplex.json", "num_machines": 8 },
    # {"path_in_str": "MediumComplex.json", "num_machines": 10 },
    {"path_in_str": "MediumComplex.json", "num_machines": 16 },
    {"path_in_str": "MediumComplex.json", "num_machines": 20 },
    # {"path_in_str": "xlargeComplex.json", "num_machines": 20 },
    # {"path_in_str": "xlargeComplex.json", "num_machines": 26 },
    # {"path_in_str": "xlargeComplex.json", "num_machines": 32 },
    # {"path_in_str": "xlargeComplex.json", "num_machines": 40 },
    {"path_in_str": "xlargeComplex.json", "num_machines": 48 },
    
]


if rank == 0:
    gdown.download_folder(id="1hwZZT1IgYVfc6dKbM3mDP9ygR2vmhUAQ")

comm.Barrier()

old_path_in_str = ""
for run in runs:
    path_in_str = run["path_in_str"]
    num_machines = run["num_machines"]
    
    if old_path_in_str != path_in_str:
        graph, durations = load_dag_from_json_rx("./Graphs/" + path_in_str)

    if rank == 0:
        start_time = timeit.default_timer()
        logger(f"Started work at graph {path_in_str}")

    if rank == 0:
        node_list = leveled_topological_sort(graph)
        # print(f"Node list for machine 0 = {node_list}")
        node_list_split = split_list_into_sublists_with_remainder(node_list, size)
    else:
        node_list_split = None



    node_list_split = comm.scatter(node_list_split, root=0)

    # print(f"Node list for machine {rank} = {node_list_split}")

    result = allocate_jobs_to_machines_with_heuristic_rx((graph, durations), node_list_split, 8)

    result = comm.gather(result, root=0)

    old_path_in_str = path_in_str
    
    if rank == 0:
        filename = "result_" + path_in_str + "_n=" + str(num_machines)
        with open(filename , "w") as f:
            json.dump(result, f)
        bucket.upload_local_file(
            local_file="./" + filename,
            file_name=filename,
            file_infos={},
        )
        elapsed = timeit.default_timer() - start_time
        perflogs.append({"filename": path_in_str, "elapsed": elapsed, "num_machines": num_machines})
        logger(json.dumps(perflogs))
            
    comm.Barrier()
    
if rank == 0:
    with open("additional_info.json", "w") as f:
        json.dump(perflogs, f)
        bucket.upload_local_file(
            local_file="./additional_info.json",
            file_name="additional_info.json",
            file_infos={},
        )