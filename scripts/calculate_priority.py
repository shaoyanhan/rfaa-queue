# 小根堆
def hhsearch_priority(task_element):
    weight = {"time": 0.5, "mem": 0.2, "len": 0.3}
    priority = weight["time"] * task_element.time + weight["mem"] * task_element.mem + weight["len"] * task_element.len

    return priority


def psipred_priority(task_element):
    weight = {"time": 0.5, "mem": 0.2, "len": 0.3}
    priority = weight["time"] * task_element.time + weight["mem"] * task_element.mem + weight["len"] * task_element.len

    return priority


def signalp6_priority(task_element):
    weight = {"mem": 0.4, "len": 0.6}
    priority = weight["mem"] * task_element.mem + weight["len"] * task_element.len

    return priority


def hhblits_bfd_priority(task_element):
    weight = {"time": 0.5, "mem": 0.3, "len": 0.2}
    priority = weight["time"] * task_element.time + weight["mem"] * task_element.mem + weight["len"] * task_element.len

    return priority


def hhblits_uniref_1_priority(task_element):
    weight = {"time": 0.4, "mem": 0.4, "len": 0.2}
    priority = weight["time"] * task_element.time + weight["mem"] * task_element.mem + weight["len"] * task_element.len

    return priority


def hhblits_uniref_2_priority(task_element):
    weight = {"time": 0.3, "mem": 0.4, "len": 0.3}
    priority = weight["time"] * task_element.time + weight["mem"] * task_element.mem + weight["len"] * task_element.len

    return priority


def hhblits_uniref_3_priority(task_element):
    weight = {"time": 0.2, "mem": 0.4, "len": 0.4}
    priority = weight["time"] * task_element.time + weight["mem"] * task_element.mem + weight["len"] * task_element.len

    return priority



def normal_priority(task_element):

    return task_element.time


def excess_priority(task_element):

    return task_element.time


# 大根堆
def suspend_priority(task_element):

    return (task_element.time * -1)
    

queue_type_to_function = {
    "hhsearch": hhsearch_priority,
    "psipred": psipred_priority,
    "signalp6": signalp6_priority,
    "hhblits_bfd": hhblits_bfd_priority,
    "hhblits_uniref_1": hhblits_uniref_1_priority,
    "hhblits_uniref_2": hhblits_uniref_2_priority,
    "hhblits_uniref_3": hhblits_uniref_3_priority,
    "normal": normal_priority,
    "excess": excess_priority,
    "suspend": suspend_priority
}


def calculate_priority(queue_type, task_element):
    if queue_type not in queue_type_to_function:
        raise ValueError(f"Invalid queue_type: {queue_type}")
    return queue_type_to_function[queue_type](task_element)
