from queue_system.config import global_config


def get_job_core_num(task_element):
    step = task_element.step
    args = global_config.get_args()
    core_num = args['job_core_num'][step]

    return core_num


def get_fasta_seq_len(fasta_file):
    with open(fasta_file, 'r') as file:
        lines = file.readlines()
        sequence = ''.join(line.strip() for line in lines if not line.startswith('>'))
        seq_length = len(sequence)

    return seq_length


def get_mem_num_with_len(seq_length, mem_cost_list):
    # 定义区间边界: [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 2000, inf]
    length_bounds = [100 * i for i in range(1, 11)] + [2000, float('inf')]
    
    # 根据序列长度找到对应区间的内存大小
    for i, bound in enumerate(length_bounds):
        if seq_length < bound:
            return mem_cost_list[i]


def get_job_mem_num(task_element):
    step = task_element.step
    fasta_seq_len = task_element.len
    args = global_config.get_args()

    mem_cost_list = args['job_mem_num'][step]
    mem_cost = get_mem_num_with_len(fasta_seq_len, mem_cost_list)

    return mem_cost