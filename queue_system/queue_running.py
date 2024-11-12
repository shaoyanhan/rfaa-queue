import multiprocessing
import psutil
import time
import os
import signal
from scripts.calculate_priority import calculate_priority
from scripts.run_task import run_task
from queue_system.queue_ready import queue_ready
from queue_system.queue_finished import queue_finished

class QueueRunning:
    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.lock = self.manager.Lock()
        self.normal = self.manager.PriorityQueue()  # 正常运行任务
        self.excess = self.manager.PriorityQueue()  # 超限运行任务
        self.suspend = self.manager.PriorityQueue() # 暂时挂起任务

    # 将任务添加到正常队列，并执行任务
    def add_to_normal(self, task_element):
        task_element.priority = calculate_priority('normal', task_element.params)
        with self.lock:
            self.normal.put(task_element)
        # 执行任务
        run_task(task_element)

    def move_to_excess(self, task_element):
        task_element.priority = calculate_priority('excess', task_element.params)
        with self.lock:
            if task_element in self.normal:
                self.normal.remove(task_element)
            self.excess.append(task_element)

    def is_excess(self, task_element):
        # 检查任务实时占用内存是否超过预设值
        if task_element.mem < self.get_task_memory_usage(task_element.pid):
            return True
        return False

    def check_excess_and_move(self):
        for task_element in self.normal:
            if self.is_excess(task_element):
                self.move_to_excess(task_element)

    def suspend_task(self, task_element):
        # 暂时挂起任务
        self.suspend_task_process_tree(task_element.pid)
        task_element.priority = calculate_priority('suspend', task_element.params)
        with self.lock:
            if task_element in self.normal:
                self.normal.remove(task_element)
            elif task_element in self.excess:
                self.excess.remove(task_element)
            self.suspend.append(task_element)

    def resume_task(self, task_element):
        # 恢复挂起任务
        with self.lock:
            if not task_element in self.suspend:
                return
            self.resume_task_process_tree(task_element.pid)
            task_element.priority = calculate_priority('normal', task_element.params)
            self.suspend.remove(task_element)
            self.normal.append(task_element)

    def kill_a_task(self):
        with self.lock:
            if not self.normal.empty():
                task_element = self.normal.get()
            elif not self.excess.empty():
                task_element = self.excess.get()
            elif not self.suspend.empty():
                task_element = self.suspend.get()
            else:
                return
            
            # 杀死任务
            self.kill_task_process_tree(task_element.pid)

            # 加入完成队列回收资源
            queue_finished.add_task(task_element)

            # 放回就绪队列等待调度
            queue_ready.add_task(task_element)


    def finish_task(self, task_element):
        # 任务结束，移出运行队列
        with self.lock:
            if task_element in self.normal:
                self.normal.remove(task_element)
            elif task_element in self.excess:
                self.excess.remove(task_element)
            elif task_element in self.suspend:
                self.suspend.remove(task_element)

    def is_empty(self):
        with self.lock:
            return self.normal.empty() and self.excess.empty() and self.suspend.empty()
        
    def get_task_memory_usage(pid):
        try:
            main_process = psutil.Process(pid)
            processes = [main_process] + main_process.children(recursive=True)
            print(f"process: {processes}")
            total_memory = sum(proc.memory_info().rss for proc in processes)
            total_memory_gb = total_memory / (1024 ** 3)
            return total_memory_gb
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} does not exist.")
            return None
    
    def kill_task_process_tree(pid):
        """彻底杀死指定进程及其所有子进程"""
        try:
            process = psutil.Process(pid)
            for child in process.children(recursive=True):
                os.kill(child.pid, signal.SIGTERM)  # 终止子进程
            os.kill(pid, signal.SIGTERM)  # 终止主进程
            print(f"Killed process tree with root PID {pid}")
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} does not exist.")

    def suspend_task_process_tree(pid):
        """暂停指定进程及其所有子进程"""
        try:
            process = psutil.Process(pid)
            for child in process.children(recursive=True):
                os.kill(child.pid, signal.SIGSTOP)  # 暂停子进程
            os.kill(pid, signal.SIGSTOP)  # 暂停主进程
            print(f"Suspended process tree with root PID {pid}")
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} does not exist.")

    def resume_task_process_tree(pid):
        """恢复指定进程及其所有子进程"""
        try:
            process = psutil.Process(pid)
            for child in process.children(recursive=True):
                os.kill(child.pid, signal.SIGCONT)  # 恢复子进程
            os.kill(pid, signal.SIGCONT)  # 恢复主进程
            print(f"Resumed process tree with root PID {pid}")
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} does not exist.")

    def get_total_memory_usage(self):
        with self.lock:
            total_memory = 0
            for task_element in self.normal:
                total_memory += self.get_task_memory_usage(task_element.pid)
            for task_element in self.excess:
                total_memory += self.get_task_memory_usage(task_element.pid)
            for task_element in self.suspend:
                total_memory += self.get_task_memory_usage(task_element.pid)
            return total_memory
        
    def get_task_io_usage(task):
        try:
            # 获取主进程
            main_process = psutil.Process(task.pid)
            initial_io = 0
            final_io = 0
            interval = 1  # 检测时间2秒

            # 计算初始的IO使用量
            if main_process.is_running():
                io_counters = main_process.io_counters()
                initial_io += io_counters.read_bytes + io_counters.write_bytes

            # 获取子进程的初始IO
            for child in main_process.children(recursive=True):
                if child.is_running():
                    child_io = child.io_counters()
                    initial_io += child_io.read_bytes + child_io.write_bytes

            # 等待指定的时间间隔
            time.sleep(interval)

            # 计算间隔后的IO使用量
            if main_process.is_running():
                io_counters = main_process.io_counters()
                final_io += io_counters.read_bytes + io_counters.write_bytes

            # 获取子进程的间隔后IO
            for child in main_process.children(recursive=True):
                if child.is_running():
                    child_io = child.io_counters()
                    final_io += child_io.read_bytes + child_io.write_bytes

            # 计算IO变化率
            io_rate = (final_io - initial_io) / interval  # 每秒字节数
            print(f"任务 {task.id} 的单位时间IO使用量: {io_rate}")
            return io_rate
        
        except psutil.NoSuchProcess:
            print(f"任务 {task.id} 的进程 {task.pid} 不存在")
            return 0

    def get_a_high_io_task(self):
        with self.lock:
            # 依次遍历正常队列和超限队列并返回IO使用率最高的任务
            high_io_task = None
            high_io_rate = 0
            if not self.normal.empty():
                for task_element in self.normal:
                    io_rate = self.get_task_io_usage(task_element)
                    if io_rate > high_io_rate:
                        high_io_rate = io_rate
                        high_io_task = task_element
            elif not self.excess.empty():
                for task_element in self.excess:
                    io_rate = self.get_task_io_usage(task_element)
                    if io_rate > high_io_rate:
                        high_io_rate = io_rate
                        high_io_task = task_element
            else:
                return None
            return high_io_task


# 单例模式
queue_running = QueueRunning()