import os
import yaml

from queue_system.task_element import TaskElement
from queue_system.queue_ready import queue_ready

def initialize_queue(args):
    input_config_path = args["input_config_path"]
    output_path = args["output_path"]
    initial_step = args["initial_step"]

    # 首先读入每个配置文件并创建任务元素
    # 检查input_config_path路径是否存在
    if not os.path.exists(input_config_path):
        print(f"Error: Input configuration path '{input_config_path}' not found.")
        return


    # 遍历 input_config_path 中的所有 yaml 文件
    for filename in os.listdir(input_config_path):

        if not filename.endswith(".yaml"):
            continue

        filepath = os.path.join(input_config_path, filename)
        with open(filepath, 'r') as file:
            try:
                config_data = yaml.safe_load(file)
                job_name = config_data.get("job_name", "Unnamed Job")

                # 遍历 protein_inputs 中的 fasta_file 路径并创建任务
                protein_inputs = config_data.get("protein_inputs", {})
                for protein_index, details in protein_inputs.items():
                    fasta_file = details.get("fasta_file")

                    # 检查 fasta_file 路径是否存在，如果不存在则报错并结束
                    if not os.path.exists(fasta_file):
                        print(f"Error: Fasta file '{fasta_file}' not found.")
                        return
    
                    job_output_path = os.path.join(output_path, job_name, protein_index)
                    if fasta_file:
                        task_params = {
                            "job_name": job_name,
                            "job_output_path": job_output_path,
                            "fasta_file": fasta_file
                        }
                        task_element = TaskElement(initial_step, task_params)
                        queue_ready.add_task(task_element)
            except yaml.YAMLError as e:
                print(f"Error reading {filename}: {e}")

    return