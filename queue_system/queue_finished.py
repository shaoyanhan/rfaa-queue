import multiprocessing

class QueueFinished:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QueueFinished, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def __init__(self):
        if QueueFinished._instance is not None:
            raise Exception("This class is a singleton! Use the 'queue_finished' instance.")

    def _initialize(self):
        self.manager = multiprocessing.Manager()
        self.finished = self.manager.Queue()
        self.lock = self.manager.Lock()

    def add_task(self, task_element):
        with self.lock:
            print(f"任务 {task_element.params["job_name"]} 已完成 {task_element.step} 步骤并加入完成队列")
            self.finished.put(task_element)

    def get_task(self):
        with self.lock:
            return self.finished.get()
        
    def is_empty(self):
        with self.lock:
            return self.finished.empty()
        
# 单例实例
queue_finished = QueueFinished()