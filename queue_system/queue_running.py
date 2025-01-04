import multiprocessing
import psutil
import time
import os
import signal
from scripts.calculate_priority import calculate_priority
from scripts.run_task import run_task
from queue_system.queue_ready import queue_ready
from queue_system.queue_finished import queue_finished
from queue_system.task_scheduler import task_scheduler

class QueueRunning:
    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.lock = self.manager.Lock()
        self.normal = self.manager.PriorityQueue()  # 正常运行任务
        self.excess = self.manager.PriorityQueue()  # 超限运行任务
        self.suspend = self.manager.PriorityQueue() # 暂时挂起任务
        self.dict_normal = self.manager.dict() # 用于快速判断任务是否在队列中
        self.dict_excess = self.manager.dict()
        self.dict_suspend = self.manager.dict()

    def copy_queue(self, priority_queue):
        """非破坏性地复制队列"""
        temp_list = []

        # 从队列中取出所有元素存入临时列表
        temp_list.append(priority_queue.get_nowait())
        
        # 将内容重新放回原队列
        for item in temp_list:
            priority_queue.put(item)

        # 返回队列的副本（列表形式）
        return temp_list

    def add_task(self, queue_name, task_element):
        """
        将一个 task_element 添加到指定的队列中。
        :param queue_name: 队列名称 ('normal', 'excess', 'suspend')
        :param task_element: 要添加的任务元素
        """
        with self.lock:
            queue = getattr(self, queue_name, None)
            queue_dict = getattr(self, f"dict_{queue_name}", None)
            
            if queue and queue_dict is not None:
                task_element.priority = calculate_priority(queue_name, task_element)
                task_element.update_time()
                queue.put(task_element)
                queue_dict[task_element.id] = task_element
            else:
                raise ValueError(f"Invalid queue name: {queue_name}")


    # 将任务添加到正常队列，并执行任务
    def add_to_normal(self, task_element):
        print(f"任务 {task_element.id} 加入normal队列")
        self.add_task("normal", task_element)
        # 执行任务
        run_task(task_element)


    def remove_task(self, queue_name, task_element):
        """
        将一个 task_element 从指定的队列中删除。
        :param queue_name: 队列名称 ('normal', 'excess', 'suspend')
        :param task_element: 要删除的任务元素
        """
        with self.lock:
            queue = getattr(self, queue_name, None)
            queue_dict = getattr(self, f"dict_{queue_name}")

            if queue and queue_dict is not None:
                # 重新创建队列，将目标元素排除
                temp_queue = []
                while not queue.empty():
                    item = queue.get()
                    if item != task_element:
                        temp_queue.append(item)
                # 将剩余元素重新放回队列
                for item in temp_queue:
                    queue.put(item)
                # 从字典中删除任务元素
                if task_element.id in queue_dict:
                    del queue_dict[task_element.id]
            else:
                raise ValueError(f"Invalid queue name: {queue_name}")
            

    # def remove_task(self, queue, task_element):
    #     with self.lock:
    #         temp_queue = []
    #         while not queue.empty():
    #             element = queue.get()
    #             if element != task_element:
    #                 temp_queue.append(element)
    #         for element in temp_queue:
    #             queue.put(element)

    def move_task(self, from_queue_name, to_queue_name, task_element):
        """
        将一个 task_element 从一个队列移动到另一个队列。
        :param from_queue_name: 源队列名称 ('normal', 'excess', 'suspend')
        :param to_queue_name: 目标队列名称 ('normal', 'excess', 'suspend')
        :param task_element: 要移动的任务元素
        """
        with self.lock:
            # 先从源队列移除
            self.remove_task(from_queue_name, task_element)
            # 再添加到目标队列
            self.add_task(to_queue_name, task_element)


    # def move_to_excess(self, task_element):
    #     task_element.priority = calculate_priority('excess', task_element)
    #     print(f"任务 {task_element.pid} 移入超限队列, 优先级: {task_element.priority}")
    #     with self.lock:
    #         if task_element in self.normal:
    #             self.remove_task(self.normal, task_element)
    #             print(f"任务 {task_element.pid} 从正常队列移出")
    #         task_element.update_time()
    #         self.excess.append(task_element)
    #         print(f"任务 {task_element.pid} 移入超限队列成功")

    def is_excess(self, task_element):
        # 检查任务实时占用内存是否超过预设值
        if task_element.mem < self.get_task_memory_usage(task_element.pid):
            return True
        return False

    def check_excess_and_move(self):
        # 检查正常队列中的任务是否超限，若超限则移入超限队列
        copy_normal = self.copy_queue(self.normal)

        for task_element in copy_normal:
            print(f"检查任务 {task_element.id} 是否超限")
            if self.is_excess(task_element):
                print(f"任务 {task_element.id} 超限，移入超限队列")
                # self.move_to_excess(task_element)
                self.move_task("normal", "excess", task_element)

    def suspend_task(self, task_element):
        # 暂时挂起任务
        # TODO:还需要恢复CPU占用
        self.suspend_task_process_tree(task_element.pid)
        task_element.priority = calculate_priority('suspend', task_element)
        with self.lock:
            if task_element in self.normal:
                self.remove_task(self.normal, task_element)
            else:
                self.remove_task(self.excess, task_element)
            # task_element.update_time()
            # self.suspend.append(task_element)
            self.add_task("suspend", task_element)
            task_scheduler.recycle_core(task_element.core) # 回收CPU资源


    def resume_task(self, task_element):
        # 恢复挂起任务
        # TODO: 恢复前查看占用内存大小，根据task_element.mem判断放回normal还是excess，并且分配CPU资源
        with self.lock:
            self.resume_task_process_tree(task_element.pid)

            # 检查任务的内存占用并放回相应队列
            if self.is_excess(task_element):
                task_element.priority = calculate_priority('excess', task_element)
                self.excess.append(task_element)
            else:

            task_element.priority = calculate_priority('normal', task_element)
            task_element.update_time()
            self.normal.append(task_element)

    def kill_a_task(self):
        with self.lock:
            if not self.normal.empty():
                print("normal队列不为空，取出一个任务")
                task_element = self.normal.get()
            elif not self.excess.empty():
                print("normal队列为空，excess队列不为空，取出一个任务")
                task_element = self.excess.get()
            elif not self.suspend.empty():
                print("normal队列、excess队列为空，suspend队列不为空，取出一个任务")
                task_element = self.suspend.get()
            else:
                print("所有队列为空，无任务可取")
                return
            
            # 杀死任务
            self.kill_task_process_tree(task_element.pid)

            # 加入完成队列回收资源
            queue_finished.add_task(task_element)

            # 放回就绪队列等待调度
            # TODO: 修改任务预分配内存为被杀死时的占用内存大小
            queue_ready.add_task(task_element)


    def finish_task(self, task_element):
        # 任务结束，移出运行队列
        with self.lock:
            if task_element in self.normal:
                self.remove_task(self.normal, task_element)
            elif task_element in self.excess:
                self.remove_task(self.excess, task_element)
            elif task_element in self.suspend:
                self.remove_task(self.suspend, task_element)
            queue_finished.add_task(task_element)

    def is_empty(self):
        with self.lock:
            return self.normal.empty() and self.excess.empty() and self.suspend.empty()
        
    def get_task_memory_usage(pid):
        try:
            main_process = psutil.Process(pid)
            processes = [main_process] + main_process.children(recursive=True)
            print(f"Processes list of pid {pid}: {processes}")
            total_memory = sum(proc.memory_info().rss for proc in processes)
            total_memory_gb = total_memory / (1024 ** 3)
            print(f"Process with PID {pid} uses {total_memory_gb} GB memory.")
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
                print("normal队列不为空, 遍历normal队列是否有高IO任务")
                copy_normal = self.copy_queue(self.normal)
                for task_element in copy_normal:
                    io_rate = self.get_task_io_usage(task_element)
                    if io_rate > high_io_rate:
                        high_io_rate = io_rate
                        high_io_task = task_element
            elif not self.excess.empty():
                print("normal队列为空, 遍历excess队列是否有高IO任务")
                copy_excess = self.copy_queue(self.excess)
                for task_element in copy_excess:
                    io_rate = self.get_task_io_usage(task_element)
                    if io_rate > high_io_rate:
                        high_io_rate = io_rate
                        high_io_task = task_element
            else:
                print("normal队列和excess队列均为空，无法获取高IO任务")
                return None
            return high_io_task


# 单例模式
queue_running = QueueRunning()