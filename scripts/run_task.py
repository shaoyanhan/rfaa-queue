import os
from multiprocessing import Process
from scripts.msa_hhblits_bfd import run_hhblits_bfd
from scripts.msa_hhblits_uniref import run_hhblits_uniref
from scripts.msa_hhsearch import run_hhsearch
from scripts.msa_psipred import run_psipred
from scripts.msa_signalp6 import run_signalp6
from queue_system.config import global_config

log_file_map = {
    'hhsearch': 'hhsearch.log',
    'psipred': 'psipred.log',
    'signalp6': 'signalp6.log',
    'hhblits_bfd': 'hhblits_bfd.log',
    'hhblits_uniref_1': 'hhblits_uniref_1.log',
    'hhblits_uniref_2': 'hhblits_uniref_2.log',
    'hhblits_uniref_3': 'hhblits_uniref_3.log',
}

def run_task(task_element):
    step = task_element.step
    params = task_element.params
    out_dir = params['job_output_path']
    in_fasta = params['fasta_file']
    
    args = global_config.get_args()
    cpu = args['job_core_num'][step]
    mem = args['max_job_mem_num']
    log_path = args['log_path']
    log_file = os.path.join(log_path, log_file_map[step])

    if step == 'signalp6':
        # run_signalp6(out_dir, in_fasta, log_file)
        target_function = run_signalp6
        function_args = (out_dir, in_fasta, log_file)

    elif step == 'hhblits_uniref_1' or step == 'hhblits_uniref_2' or step == 'hhblits_uniref_3':
        db_ur30 = args['db_uniref_path']
        e_value = params['e_value']
        # run_hhblits_uniref(out_dir, in_fasta, cpu, mem, db_ur30, e_value, log_file)
        target_function = run_hhblits_uniref
        function_args = (out_dir, in_fasta, cpu, mem, db_ur30, e_value, log_file)

    elif step == 'hhblits_bfd':
        db_bfd = args['db_bfd_path']
        e_value = params['e_value']
        # run_hhblits_bfd(out_dir, in_fasta, cpu, mem, db_bfd, e_value, log_file)
        target_function = run_hhblits_bfd
        function_args = (out_dir, in_fasta, cpu, mem, db_bfd, e_value, log_file)

    elif step == 'hhsearch':
        db_pdb70 = args['db_pdb70_path']
        # run_hhsearch(out_dir, cpu, mem, db_pdb70, log_file)
        target_function = run_hhsearch
        function_args = (out_dir, cpu, mem, db_pdb70, log_file)

    elif step == 'psipred':
        pipe_dir = args['rfaa_pipe_path']
        # run_psipred(out_dir, pipe_dir, log_file)
        target_function = run_psipred
        function_args = (out_dir, pipe_dir, log_file)

    p = Process(target=target_function, args=function_args) # 创建进程
    task_element.pid = p.pid # 记录进程ID
    p.start() # 启动进程
    print(f"Running task: {task_element}, PID: {task_element.pid}")

    return