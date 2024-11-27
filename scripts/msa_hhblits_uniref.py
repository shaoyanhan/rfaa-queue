import os
import subprocess

from queue_system.queue_finished import queue_finished
from queue_system.queue_ready import queue_ready
from scripts.utilities import get_job_mem_num, get_job_core_num

def task_complete(task_element, terminate):
    print(f'{task_element.step} step of {task_element.params["job_name"]} finished')

    # 将任务加入finished队列等待资源回收
    queue_finished.add_task(task_element)

    # 已经得到充足数量的msa，进行psipred操作
    if terminate:
        # 修改任务类型
        task_element.step = "psipred"

    # 获取任务所需的内存和核心数
    task_element.mem = get_job_mem_num(task_element)
    task_element.core = get_job_core_num(task_element)
    print(f'任务{task_element}参数修改完毕，加入ready队列等待资源分配')
    
    # 将任务加入ready队列等待资源分配
    queue_ready.add_task(task_element)


def run_hhblits_uniref(out_dir, in_fasta, cpu, mem, db_ur30, e_value, log_file, task_element):
    # 标识目前是否需要继续下一步hhblits操作
    terminate = False

    # e_value的列表，其中也包括了 hhblits_bfd 的，因为最后切换任务需要
    e_value_list = [1e-10, 1e-6, 1e-3, 1e-3]

    final_msa = os.path.join(out_dir, "t000_.msa0.a3m")
    tmp_dir = os.path.join(out_dir, "hhblits")
    os.makedirs(tmp_dir, exist_ok=True)

    HHBLITS_UR30 = f"hhblits -o /dev/null -mact 0.35 -maxfilt 100000000 -neffmax 20 -cov 25 -cpu {cpu} -nodiff -realign_max 100000000 -maxseq 1000000 -maxmem {mem} -n 4 -d {db_ur30}"

    if not os.path.exists(final_msa):
        # <<< Run HHblits against UniRef30 >>>
        print(f"Running HHblits against UniRef30 with E-value cutoff {e_value}")
        a3m_file = os.path.join(tmp_dir, f"t000_.{e_value}.a3m")
        if not os.path.exists(a3m_file):
            cmd = f"""
            {HHBLITS_UR30} -i {in_fasta} -oa3m {a3m_file} -e {e_value} -v 0
            """
            print(cmd)
            subprocess.run(cmd, shell=True, check=True)
        else:
            print(f"Found {a3m_file}, skipping HHblits against UniRef30 with E-value cutoff {e_value}.")


        # <<< Run hhfilter with 90% identity and 75% coverage >>>
        a3m_file_id90cov75 = os.path.join(tmp_dir, f"t000_.{e_value}.id90cov75.a3m")
        if not os.path.exists(a3m_file_id90cov75):
            print(f"Running hhfilter on {final_msa} with E-value cutoff {e_value}, 90% identity, and 75% coverage")
            cmd = f"""
            hhfilter -maxseq 100000 -id 90 -cov 75 -i {a3m_file} -o {a3m_file_id90cov75}
            """
            print(cmd)
            subprocess.run(cmd, shell=True, check=True)
        else:
            print(f"Found {a3m_file_id90cov75}, skipping HHfilter with 90% identity and 75% coverage.")
            
        # <<< Check the number of sequences in the filtered a3m file with 90% identity and 75% coverage >>>
        n75 = int(subprocess.getoutput(f"grep -c '^>' {a3m_file_id90cov75}"))
        # copy the filtered a3m file to the output directory if the number of sequences is greater than the threshold
        if n75 > 2000 and not os.path.exists(final_msa):
            os.system(f"cp {a3m_file_id90cov75} {final_msa}")
            print(f"Found {n75} sequences in {a3m_file_id90cov75}, finishing the HHblits process.")
            terminate = True
            task_complete(task_element, terminate)
            return


        # <<< Run hhfilter with 90% identity and 50% coverage >>>
        a3m_file_id90cov50 = os.path.join(tmp_dir, f"t000_.{e_value}.id90cov50.a3m")
        if not os.path.exists(a3m_file_id90cov50):
            print(f"Running hhfilter on {final_msa} with E-value cutoff {e_value}, 90% identity, and 50% coverage")
            cmd = f"""
            hhfilter -maxseq 100000 -id 90 -cov 50 -i {a3m_file} -o {a3m_file_id90cov50}
            """
            print(cmd)
            subprocess.run(cmd, shell=True, check=True)
        else:
            print(f"Found {a3m_file_id90cov50}, skipping HHfilter with 90% identity and 50% coverage.")

        # <<< Check the number of sequences in the filtered a3m file with 90% identity and 50% coverage >>>
        n50 = int(subprocess.getoutput(f"grep -c '^>' {a3m_file_id90cov50}"))
        # copy the filtered a3m file to the output directory if the number of sequences is greater than the threshold
        if n50 > 4000 and not os.path.exists(final_msa):
            os.system(f"cp {a3m_file_id90cov50} {final_msa}")
            print(f"Found {n50} sequences in {a3m_file_id90cov50}, breaking the loop.")
            terminate = True
            task_complete(task_element, terminate)
            return
        

        # 没有得到足够数量的msa，继续下一步hhblits操作
        params = task_element.params
        # 修改fasta_file参数为上一步生成的a3m文件
        params["fasta_file"] = a3m_file_id90cov50
        # 检测下一个e_value
        next_e_value = e_value_list[e_value_list.index(e_value) + 1]

        # 根据e_value的值修改下一步任务类型
        if next_e_value == e_value: # 如果下一个e_value和当前e_value相同, 则进行hhblits_bfd操作
            task_element.step = "hhblits_bfd"
        else:
            params["e_value"] = next_e_value
            task_element.step = f"hhblits_uniref_{e_value_list.index(e_value) + 1}"

        task_element.params = params
        task_complete(task_element, terminate)
        
        

    else:
        print(f"Found final result file: {final_msa}, skipping HHblits process.")
        terminate = True
        task_complete(task_element, terminate)
        return