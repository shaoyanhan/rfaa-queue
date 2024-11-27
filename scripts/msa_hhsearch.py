import os
import subprocess

from queue_system.queue_finished import queue_finished


def task_complete(task_element):
    print(f'{task_element.step} step of {task_element.params["job_name"]} finished')

    # 将任务加入finished队列等待资源回收
    queue_finished.add_task(task_element)

    print(f'all steps of {task_element.params["job_name"]} finished')


def run_hhsearch(out_dir, cpu, mem, db_pdb70, log_file, task_element):
    out_prefix = os.path.join(out_dir, "t000_")
    final_msa = f"{out_prefix}.msa0.a3m"
    
    if os.path.exists(final_msa):
        if os.path.exists(f"{out_prefix}.hhr") and os.path.exists(f"{out_prefix}.atab"):
            print(f"Found {out_prefix}.hhr and {out_prefix}.atab, skipping HHsearch.")
            return
        
        print("Running hhsearch")
        HH = f"hhsearch -b 50 -B 500 -z 50 -Z 500 -mact 0.05 -cpu {cpu} -maxmem {mem} -aliw 100000 -e 100 -p 5.0 -d {db_pdb70}"
        cmd = f"""
        cat {out_prefix}.ss2 {out_prefix}.msa0.a3m > {out_prefix}.msa0.ss2.a3m
        {HH} -i {out_prefix}.msa0.ss2.a3m -o {out_prefix}.hhr -atab {out_prefix}.atab -v 0
        """
        print(cmd)
        subprocess.run(cmd, shell=True, check=True)

    else:
        print(f"Missing {final_msa}, stopping HHsearch.")

    task_complete(task_element)