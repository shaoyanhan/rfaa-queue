from scripts.load_arguments import load_arguments
from scripts.initialize_queue import initialize_queue
from queue_system.config import global_config
from queue_system.task_scheduler import task_scheduler

def main():
    # 读入用户输入参数以及默认参数
    args = load_arguments()

    # 将参数存入全局配置
    global_config.set_args(args)

    # 初始化队列系统
    initialize_queue(args)

    # 启动任务调度器
    task_scheduler.monitor()

if __name__ == "__main__":
    main()