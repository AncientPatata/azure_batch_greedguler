from mpi4py import MPI
import numpy as np
import json 
import rustworkx as rx
import timeit
from datetime import datetime, timedelta
from log import *


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def load_dag_from_json_rx(filepath):
    print("Loading DAG from JSON file " + filepath + "....")  # TODO: Custom logging with control of verbosity.
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
    node_queue = [[n for n in graph.node_indices() if graph.in_degree(n) == 0]]
    not_done= True 
    while not_done:
        successors = []
        out_edges = []
        for q in node_queue:
            for n in graph.successor_indices(q):
                out_edges.append((q,n))
                successors.append(n)
        successors = set(successors)
        
        graph.remove_edges_from(out_edges)
        graph.remove_nodes_from(node_queue)

        node_queue = [n for n in successors if graph.in_degree(n) == 0]

def earliest_start_time_optimized(task, graph, schedule):
    # Calculate earliest start time for a task on a machine respecting dependencies
    print("task : ", task)
    print("schedule :", schedule) 
    dependencies = list(graph.predecessors(task))
    print(dependencies)
    if not dependencies:
        return 0
    else:
        max_end_time = max([job['end_time'] for machine_schedule in schedule for job in machine_schedule if job['job_index'] in dependencies])
        return max_end_time

def allocate_jobs_to_machines_with_heuristic_rx(graph: (rx.PyDiGraph, dict),node_list, num_machines=8):
    man_graph = graph[0].copy()
    durations = graph[1]
    jobs = {}

    free_time = [0] * num_machines


    for queue in node_list:
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

        # TODO: Do this but for this 
        successors = []
        out_edges = []
        for q in queue:
            for n in man_graph.successor_indices(q):
                out_edges.append((q,n))
                successors.append(n)
        successors = set(successors)
    
        man_graph.remove_edges_from(out_edges)
        man_graph.remove_nodes_from(queue)
        # print("QUEUE: ", queue)
        # def edge_filter(edge):
        #     print("EDGE: ", edge)
        #     return edge[0] in queue

        queue = [n for n in successors if man_graph.in_degree(n) == 0]
        
def split_list_into_sublists_with_remainder(lst, n):
    # Split list into equal sublists of size 'n'
    sublists = [lst[i:i + n] for i in range(0, len(lst) - len(lst) % n, n)]
    
    # Check for any remaining elements that didn't fit into the equal sublists
    remainder = lst[len(lst) - len(lst) % n:]
    
    # If there is a remainder, append it to the last sublist
    if remainder:
        if sublists:
            sublists[-1].extend(remainder)
        else:
            sublists.append(remainder)
    
    return sublists


## Upload results to backblaze storage bucket (Azure storage permission problem..)
if rank == 0:
    from b2sdk.v2 import *

    info = InMemoryAccountInfo()

    b2_api = B2Api(info)

    application_key_id = "005a863833255a00000000001"

    application_key = "K005KxufnOehfkMKAQZJnKftPeRkyrA"

    b2_api.authorize_account("production", application_key_id, application_key)
    bucket = b2_api.get_bucket_by_name("greedguler")


logger = create_logging_function(rank)

logger("Started work")

graph, durations = load_dag_from_json_rx("./smallComplex.json")

if rank == 0:
    node_list = leveled_topological_sort(graph)
    node_list_split = split_list_into_sublists_with_remainder(node_list, size)


