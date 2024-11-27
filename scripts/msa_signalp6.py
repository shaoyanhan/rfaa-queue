import os
import subprocess

from queue_system.queue_finished import queue_finished
from queue_system.queue_ready import queue_ready
from scripts.utilities import get_job_mem_num, get_job_core_num


def task_complete(task_element):
    print(f'{task_element.step} step of {task_element.params["job_name"]} finished')

    # 将任务加入finished队列等待资源回收
    queue_finished.add_task(task_element)

    # 修改参数为下一步 hhblits_uniref_1 的相关参数
    # 修改任务类型
    task_element.step = "hhblits_uniref_1"
    # 获取任务所需的内存和核心数
    task_element.mem = get_job_mem_num(task_element)
    task_element.core = get_job_core_num(task_element)
    params = task_element.params
    # 修改任务参数
    params["e_value"] = 1e-10
    task_element.params = params
    print(f'任务{task_element}参数修改完毕，加入ready队列等待资源分配')
    
    # 将任务加入ready队列等待资源分配
    queue_ready.add_task(task_element)


def run_signalp6(out_dir, in_fasta, log_file, task_element):
    tmp_dir = os.path.join(out_dir, "signalp")
    os.makedirs(tmp_dir, exist_ok=True)

    cmd = f"""
    signalp6 --fastafile {in_fasta} --organism other --output_dir {tmp_dir} --format none --mode slow
    """    
    print(cmd)
    subprocess.run(cmd, shell=True, check=True)

    task_complete(task_element)