import multiprocessing
import psutil
import time
from queue_system.queue_ready import queue_ready
from queue_system.queue_running import queue_running
from queue_system.queue_finished import queue_finished
from queue_system.config import global_config

class TaskScheduler:
    def __init__(self):
        self.lock = multiprocessing.Manager().Lock()  # 进程间锁

        # 固定值，表示设定的资源总量
        self.total_avaliable_core = None
        self.total_avaliable_mem = None

        # 可用资源，随着任务的分配和释放而变化，小于等于固定值
        self.current_avaliable_core = None
        self.current_avaliable_mem = None

        # 固定值
        self.mem_buffer = None
        self.wait_time_max = None
        self.wait_time_mid = None

    def initialize(self):
        print("初始化监控系统参数")

        args = global_config.get_args()
        # 获取空闲核数
        core_count = psutil.cpu_count(logical=True)  
        usage_per_core = psutil.cpu_percent(interval=1, percpu=True)  # 每个核的CPU使用率

        # 空闲核数: 假设低于一定使用率的核为“空闲核”
        threshold = 10  # 设置一个使用率阈值，例如10%
        idle_cores = sum(1 for usage in usage_per_core if usage < threshold)
        print(f"空闲核数: {idle_cores} / {core_count} (空闲阈值: {threshold}%)")

        # 剩余运行内存量
        available_memory = psutil.virtual_memory().available / (1024 ** 3)
        print(f"剩余运行内存量: {available_memory:.2f} GB")

        # 设置全局参数
        user_set_total_avaliable_core = args.total_avaliable_core
        user_set_total_avaliable_mem = args.total_avaliable_mem
        self.mem_buffer = args.mem_buffer
        self.wait_time_max = args.wait_time_max
        self.wait_time_mid = args.wait_time_mid

        self.total_avaliable_core = user_set_total_avaliable_core if user_set_total_avaliable_core <= core_count else core_count
        self.total_avaliable_mem = user_set_total_avaliable_mem if user_set_total_avaliable_mem <= available_memory else available_memory
        
        self.total_avaliable_core = self.total_avaliable_core - 1  # 为监控进程预留一个核
        self.total_avaliable_mem = self.total_avaliable_mem - args.mem_buffer  # 减去内存缓冲区

        self.current_avaliable_core = self.total_avaliable_core
        self.current_avaliable_mem = self.total_avaliable_mem

        # 打印最终设置的参数
        print(f"最终总资源设置: core: {self.total_avaliable_core}核, memory: {self.total_avaliable_mem}GB")
        print(f"内存缓冲区: {self.mem_buffer}GB")
        print(f"CPU最大等待率: {self.wait_time_max}%")
        print(f"CPU中等等待率: {self.wait_time_mid}%")


    def monitor(self):
        print("启动监控系统")

        # 初次启动 monitor 时，如果就绪队列不为空，进行初始化
        if queue_ready.is_empty():
            print("就绪队列为空，无任务可调度")
            return
        
        self.initialize()

        # 连续尝试调入任务的次数
        allocate_try_times = 0

        # 监控系统初始化完成，启动监控资源状态
        print("监控系统初始化完成，开始监控资源状态")
        while True:
            # 检查是否具备结束队列系统的条件
            if queue_running.is_empty():
                if queue_ready.is_empty():
                    print("所有任务已完成")
                    break
                # 如果运行队列为空，且就绪队列不为空，并且连续尝试次数大于10次，退出
                else:
                    if allocate_try_times > 10:
                        print("运行队列为空，就绪队列不为空，连续尝试次数过多，退出")
                        break
            
            # 检查是否有任务超限
            print("检查运行队列是否有任务超限")
            queue_running.check_excess_and_move()


            # 检查内存资源状态并更新
            print("检查内存资源状态")
            memory_left = self.check_memory_left()
            if memory_left < 0:
                # 内存资源不足，尝试杀死任务
                print("内存资源不足，尝试杀死任务")
                memory_left = self.killer(memory_left)
                if memory_left < 0:
                    print("尝试杀死任务后内存资源仍不足，退出")
                    break
            print("成功杀死任务，memory_left: ", memory_left)
            self.current_avaliable_mem = memory_left

            # 检查IO资源状态
            print("检查IO资源状态")
            wa = self.check_high_io_usage()
            print(f"当前IO等待率: {wa:.2f}%")
            self.suspender(wa)

            # 检查是否有任务完成并回收预分配的CPU资源
            if not queue_finished.is_empty():
                self.collector()

            # 尝试分配任务
            if self.check_sufficient_resources():
                print("资源剩余量大于0，尝试分配任务")
                if self.allocator():
                    allocate_try_times = 0
                else:
                    allocate_try_times += 1

    # 从queue_finished中回收任务资源
    def collector(self):
        with self.lock:
            while not queue_finished.is_empty():
                task_element = queue_finished.get_task()
                print(f"任务 {task_element.id} 已完成，回收cpu资源: {task_element.cpu}")
                # 回收预分配的CPU资源，这里不计算内存资源，因为内存资源变动快，需要实时更新
                core_cost = task_element.core
                self.current_avaliable_core += core_cost

    # 从就绪队列分配任务到queue_running 的 normal 队列
    def allocator(self):
        with self.lock:
            step, task_element = queue_ready.get_task()
            if step and task_element:
                print(f"尝试分配任务 {task_element.id} 到运行队列")
                if not self.allocate_resources(task_element):
                    print("任务 {task_element.id} 的资源需求超过剩余资源，无法分配")
                    return False
                queue_running.add_to_normal(task_element)

        return True

    # 暂时挂起任务
    def suspender(self, wa):
        with self.lock:
            # 如果IO等待时间超过最大等待时间，挂起任务
            if wa >= self.wait_time_max:
                print("IO等待时间超过最大等待时间，尝试挂起任务")
                task_element = queue_running.get_a_high_io_task()
                if not task_element:
                    print("没有找到可以挂起的任务")
                    return
                print(f"挂起任务 {task_element.id}")
                queue_running.suspend_task(task_element)

            # 如果IO等待时间小于中等等待时间，恢复挂起的任务
            elif wa < self.wait_time_mid:
                print("IO等待时间小于中等等待时间，尝试恢复挂起任务")
                if not queue_running.suspend.empty():
                    task_element = queue_running.suspend.get()
                    queue_running.resume_task(task_element)
                    return
                print("没有挂起的任务可以恢复")


    # 终止任务
    def killer(self, memory_left):
        with self.lock:
            # 一直杀死任务，直到内存资源足够
            kill_try_times = 0
            while True:
                queue_running.kill_a_task()
                memory_left = self.check_memory_left()
                kill_try_times += 1
                if memory_left >= 0 or kill_try_times > 10:
                    print("杀死任务结束, memory_left: ", memory_left, "kill_try_times: ", kill_try_times)
                    break
            return memory_left


    def check_memory_left(self):
        total_memory_usage = queue_running.get_total_memory_usage()
        print(f"当前内存使用量: {total_memory_usage:.2f} GB")
        memory_left = self.total_avaliable_mem - total_memory_usage
        print(f"剩余内存: {memory_left:.2f} GB")
        
        return memory_left

    def check_high_io_usage(self):
        # 记录5秒内的wa值
        wa_values = []
        start_time = time.time()
        duration = 5
        
        while time.time() - start_time < duration:
            # 获取IO等待时间
            cpu_times = psutil.cpu_times_percent(interval=1)
            wa_values.append(cpu_times.iowait)
        
        # 计算并返回wa的平均值
        if wa_values:
            avg_wa = sum(wa_values) / len(wa_values)
            return avg_wa
        else:
            return 0.0

    def check_sufficient_resources(self):
        # 检查内存和cpu资源是否足够
        return self.current_avaliable_core > 0 and self.current_avaliable_mem > 0
    
    
    def allocate_resources(self, task_element):
        core_cost = task_element.core
        mem_cost = task_element.mem
        
        core_left = self.current_avaliable_core - core_cost
        mem_left = self.current_avaliable_mem - mem_cost

        if core_left >= 0 and mem_left >= 0:
            self.current_avaliable_core = core_left
            self.current_avaliable_mem = mem_left
            return True
        else:
            return False
    

task_scheduler = TaskScheduler()


