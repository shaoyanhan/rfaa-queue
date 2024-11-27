# multi_level_priority_queue.py
import multiprocessing
from scripts.calculate_priority import calculate_priority

class MultiLevelPriorityQueue:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MultiLevelPriorityQueue, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def __init__(self):
        if MultiLevelPriorityQueue._instance is not None:
            raise Exception("This class is a singleton! Use the 'queue_ready' instance.")

    def _initialize(self):
        self.manager = multiprocessing.Manager()
        self.queues = {
            "hhsearch": self.manager.PriorityQueue(),
            "psipred": self.manager.PriorityQueue(),
            "signalp6": self.manager.PriorityQueue(),
            "hhblits_bfd": self.manager.PriorityQueue(),
            "hhblits_uniref_3": self.manager.PriorityQueue(),
            "hhblits_uniref_2": self.manager.PriorityQueue(),
            "hhblits_uniref_1": self.manager.PriorityQueue(),
        }
        self.lock = self.manager.Lock()

    def add_task(self, task_element):
        print(f"Adding task to ready queue: \n{task_element}")
        step = task_element.step
        if step not in self.queues:
            raise ValueError(f"Invalid step: {step}")
        task_element.priority = calculate_priority(step, task_element)
        with self.lock:
            print(f"Adding task to {step} queue: \n{task_element}")
            task_element.update_time()
            self.queues[step].put(task_element)

    def get_task(self):
        with self.lock:
            for step in self.queues:
                if self.queues[step].empty():
                    continue
                return step, self.queues[step].get()
        return None, None
    
    def is_empty(self):
        with self.lock:
            for step in self.queues:
                if not self.queues[step].empty():
                    return False
        return True

# 单例实例
queue_ready = MultiLevelPriorityQueue()
