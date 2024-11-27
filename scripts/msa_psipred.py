import os
import subprocess

from queue_system.queue_finished import queue_finished
from queue_system.queue_ready import queue_ready
from scripts.utilities import get_job_mem_num, get_job_core_num


def task_complete(task_element):
    print(f'{task_element.step} step of {task_element.params["job_name"]} finished')

    # 将任务加入finished队列等待资源回收
    queue_finished.add_task(task_element)

    # 修改参数为下一步 psipred 的相关参数
    task_element.step = "hhsearch"

    # 获取任务所需的内存和核心数
    task_element.mem = get_job_mem_num(task_element)
    task_element.core = get_job_core_num(task_element)
    print(f'任务{task_element}参数修改完毕，加入ready队列等待资源分配')
    
    # 将任务加入ready队列等待资源分配
    queue_ready.add_task(task_element)


def run_psipred(out_dir, pipe_dir, log_file, task_element):
    print("Running PSIPRED")
    out_prefix = os.path.join(out_dir, "t000_")
    final_msa = f"{out_prefix}.msa0.a3m"
    tmp_dir = os.path.join(out_dir, "log")
    os.makedirs(tmp_dir, exist_ok=True)

    if os.path.exists(final_msa):
        cmd = f"""
        {pipe_dir}/input_prep/make_ss.sh {final_msa} {out_prefix}.ss2 > {tmp_dir}/make_ss.stdout 2> {tmp_dir}/make_ss.stderr
        """
        print(cmd)
        subprocess.run(cmd, shell=True, check=True)
    else:
        print(f"Missing {final_msa}, stopping PSIPRED.")

    task_complete(task_element)