def hhsearch_priority(params):
    # TODO: implement priority calculation for HHsearch
    return 0

queue_type_to_function = {
    "hhsearch": hhsearch_priority,
    "psipred": psipred_priority,
    "signalp6": signalp6_priority,
    "hhblits_bfd": hhblits_bfd_priority,
    "hhblits_uniref_1": hhblits_uniref_priority,
    "hhblits_uniref_2": hhblits_uniref_priority,
    "hhblits_uniref_3": hhblits_uniref_priority,
    "normal": normal_priority,
    "excess": excess_priority,
    "suspend": suspend_priority
}



def calculate_priority(queue_type, params):
    if queue_type not in queue_type_to_function:
        raise ValueError(f"Invalid queue_type: {queue_type}")
    return queue_type_to_function[queue_type](params)
