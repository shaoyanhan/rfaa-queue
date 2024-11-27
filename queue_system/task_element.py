import time

class TaskElement:
    def __init__(self, step, len, params):
        self._step = step  # 初始化任务步骤
        self._len = len # 初始化蛋白序列长度
        self._params = params  # 初始化受保护的任务参数
        self._priority = None  # 初始化优先级
        self._pid = None  # 初始化进程 ID
        self._core = None  # 预分配的 core 数量
        self._mem = None  # 预分配的内存数量
        self._time = None  # 初始化时间戳

    @property
    def step(self):
        """任务步骤的 getter 方法"""
        return self._step
    
    @step.setter
    def step(self, value):
        """任务步骤的 setter 方法"""
        self._step = value

    @property
    def len(self):
        """蛋白序列长度的 getter 方法"""
        return self._len
    
    @len.setter
    def len(self, value):
        """蛋白序列长度的 setter 方法"""
        self._len = value

    @property
    def priority(self):
        """优先级的 getter 方法"""
        return self._priority

    @priority.setter
    def priority(self, value):
        """优先级的 setter 方法，确保只有内部逻辑可设置优先级"""
        if not isinstance(value, (int, float, type(None))):  # 检查是否为数字或 None
            raise ValueError("Priority must be a numeric value or None.")
        self._priority = value

    @property
    def params(self):
        """任务参数的 getter 方法"""
        return self._params

    @params.setter
    def params(self, new_params):
        """任务参数的 setter 方法，更新任务参数并重置优先级"""
        self._params = new_params
        self._priority = None  # 重置优先级为 None，表示需要重新计算

    @property
    def pid(self):
        """进程 ID 的 getter 方法"""
        return self._pid
    
    @pid.setter
    def pid(self, value):
        """进程 ID 的 setter 方法"""
        self._pid = value

    @property
    def core(self):
        """core 数量的 getter 方法"""
        return self._core
    
    @core.setter
    def core(self, value):
        """core 数量的 setter 方法"""
        self._core = value

    @property
    def mem(self):
        """内存数量的 getter 方法"""
        return self._mem
    
    @mem.setter
    def mem(self, value):
        """内存数量的 setter 方法"""
        self._mem = value

    @property
    def time(self):
        """时间戳的 getter 方法"""
        return self._time
    
    @time.setter
    def time(self, value):
        """时间戳的 setter 方法"""
        self._time = value

    def update_time(self):
        """更新时间戳"""
        timestamp = time.time()
        # 去除小数部分
        self._time = int(timestamp)

    # 重写 __repr__ 方法，用于打印任务信息 
    def __repr__(self):
        return f"TaskElement(step={self.step}, len={self.len}, priority={self.priority}, pid={self.pid}, core={self.core}, mem={self.mem}, time={self.time})"

    # 重写 __lt__ 方法，用于加入优先级队列时比较任务优先级
    def __lt__(self, other):
        if self.priority is None or other.priority is None:
            raise ValueError("Cannot compare tasks without a priority set.")
        return self.priority < other.priority  # 比较任务优先级以便队列排序